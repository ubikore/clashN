using ClashN.Tool;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace ClashN.Handler;

// PAC service behavior is based on v2rayN 6.60 PacLib.
public static class PacHandler
{
    private const string PacFileName = "pac.txt";
    private const string DefaultPacResourceName = "ClashN.Resources.pac.txt";
    private const int ReloadDelayMilliseconds = 400;
    private static readonly object SyncRoot = new();

    private static string _configPath = string.Empty;
    private static int _httpPort;
    private static int _pacPort;
    private static TcpListener? _tcpListener;
    private static string _pacText = string.Empty;
    private static int _listenerGeneration;
    private static FileSystemWatcher? _pacWatcher;
    private static Timer? _reloadTimer;
    private static Action? _pacChanged;

    public static void Start(string configPath, int httpPort, int pacPort, Action? pacChanged = null)
    {
        lock (SyncRoot)
        {
            var needRestart = _tcpListener is null
                || configPath != _configPath
                || httpPort != _httpPort
                || pacPort != _pacPort;

            _configPath = configPath;
            _httpPort = httpPort;
            _pacPort = pacPort;
            _pacText = LoadPacText(_configPath, _httpPort);

            if (!needRestart)
            {
                _pacChanged = pacChanged;
                return;
            }

            StopCore();
            _pacChanged = pacChanged;

            var listener = new TcpListener(IPAddress.Loopback, _pacPort);
            listener.Start();
            _tcpListener = listener;

            var generation = ++_listenerGeneration;
            _ = Task.Run(() => RunListenerAsync(listener, generation));
            StartPacWatcher(generation);
        }
    }

    public static void Stop()
    {
        lock (SyncRoot)
        {
            StopCore();
        }
    }

    private static string LoadPacText(string configPath, int httpPort)
    {
        var path = Path.Combine(configPath, PacFileName);
        if (!File.Exists(path))
        {
            Directory.CreateDirectory(configPath);
            File.WriteAllText(path, ReadDefaultPacText(), new UTF8Encoding(false));
        }

        return File.ReadAllText(path)
            .Replace("__PROXY__", $"PROXY 127.0.0.1:{httpPort};DIRECT;");
    }

    private static string ReadDefaultPacText()
    {
        using var stream = typeof(PacHandler).Assembly.GetManifestResourceStream(DefaultPacResourceName)
            ?? throw new InvalidOperationException($"Embedded PAC resource '{DefaultPacResourceName}' was not found.");
        using var reader = new StreamReader(stream, Encoding.UTF8, true);
        return reader.ReadToEnd();
    }

    private static async Task RunListenerAsync(TcpListener listener, int generation)
    {
        while (Volatile.Read(ref _listenerGeneration) == generation)
        {
            try
            {
                var client = await listener.AcceptTcpClientAsync();
                _ = ServeClientAsync(client);
            }
            catch (ObjectDisposedException)
            {
                break;
            }
            catch (SocketException) when (Volatile.Read(ref _listenerGeneration) != generation)
            {
                break;
            }
            catch
            {
                await Task.Delay(100);
            }
        }
    }

    private static void StartPacWatcher(int generation)
    {
        _reloadTimer = new Timer(
            _ => ReloadPacTextFromWatcher(generation),
            null,
            Timeout.Infinite,
            Timeout.Infinite);

        var watcher = new FileSystemWatcher(_configPath, PacFileName)
        {
            NotifyFilter = NotifyFilters.FileName
                | NotifyFilters.LastWrite
                | NotifyFilters.CreationTime
                | NotifyFilters.Size
        };
        watcher.Changed += OnPacFileChanged;
        watcher.Created += OnPacFileChanged;
        watcher.Deleted += OnPacFileChanged;
        watcher.Renamed += OnPacFileChanged;
        watcher.Error += OnPacWatcherError;

        _pacWatcher = watcher;
        watcher.EnableRaisingEvents = true;
    }

    private static void OnPacFileChanged(object sender, FileSystemEventArgs e)
    {
        lock (SyncRoot)
        {
            if (!ReferenceEquals(sender, _pacWatcher))
            {
                return;
            }

            _reloadTimer?.Change(ReloadDelayMilliseconds, Timeout.Infinite);
        }
    }

    private static void OnPacWatcherError(object sender, ErrorEventArgs e)
    {
        Utils.SaveLog("PAC file watcher error", e.GetException());
        OnPacFileChanged(sender, new FileSystemEventArgs(WatcherChangeTypes.Changed, _configPath, PacFileName));
    }

    private static void ReloadPacTextFromWatcher(int generation)
    {
        Exception? lastError = null;

        for (var attempt = 0; attempt < 5; attempt++)
        {
            string configPath;
            int httpPort;

            lock (SyncRoot)
            {
                if (_tcpListener is null || generation != _listenerGeneration)
                {
                    return;
                }

                configPath = _configPath;
                httpPort = _httpPort;
            }

            try
            {
                var pacText = LoadPacText(configPath, httpPort);
                lock (SyncRoot)
                {
                    if (_tcpListener is null || generation != _listenerGeneration
                        || string.Equals(_pacText, pacText, StringComparison.Ordinal))
                    {
                        return;
                    }

                    _pacText = pacText;
                    try
                    {
                        _pacChanged?.Invoke();
                    }
                    catch (Exception ex)
                    {
                        Utils.SaveLog("Refresh system PAC after file change", ex);
                    }
                }
                return;
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                lastError = ex;
                Thread.Sleep(100);
            }
        }

        if (lastError is not null)
        {
            Utils.SaveLog("Reload PAC file after change", lastError);
        }
    }

    private static async Task ServeClientAsync(TcpClient client)
    {
        try
        {
            using (client)
            {
                string pacText;
                lock (SyncRoot)
                {
                    pacText = _pacText;
                }

                var body = Encoding.UTF8.GetBytes(pacText);
                var header = Encoding.ASCII.GetBytes(
                    "HTTP/1.1 200 OK\r\n"
                    + "Content-Type: application/x-ns-proxy-autoconfig; charset=utf-8\r\n"
                    + "Cache-Control: no-cache, no-store, must-revalidate\r\n"
                    + "Pragma: no-cache\r\n"
                    + "Expires: 0\r\n"
                    + "Connection: close\r\n"
                    + $"Content-Length: {body.Length}\r\n\r\n");

                using var stream = client.GetStream();
                await ReadRequestHeadersAsync(stream);
                await stream.WriteAsync(header);
                await stream.WriteAsync(body);
                await stream.FlushAsync();
            }
        }
        catch (IOException)
        {
        }
        catch (SocketException)
        {
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static async Task ReadRequestHeadersAsync(NetworkStream stream)
    {
        var headerEnd = "\r\n\r\n"u8.ToArray();
        var buffer = new byte[1024];
        var matched = 0;
        var totalRead = 0;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        while (totalRead < 16 * 1024)
        {
            var count = await stream.ReadAsync(buffer, timeout.Token);
            if (count == 0)
            {
                return;
            }

            totalRead += count;
            for (var i = 0; i < count; i++)
            {
                matched = buffer[i] == headerEnd[matched]
                    ? matched + 1
                    : buffer[i] == headerEnd[0] ? 1 : 0;

                if (matched == headerEnd.Length)
                {
                    return;
                }
            }
        }
    }

    private static void StopCore()
    {
        ++_listenerGeneration;
        _pacWatcher?.Dispose();
        _pacWatcher = null;
        _reloadTimer?.Dispose();
        _reloadTimer = null;
        _pacChanged = null;
        _tcpListener?.Stop();
        _tcpListener = null;
    }
}
