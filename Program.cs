using System.Buffers;
using System.Diagnostics;
using System.Text;
using System.Threading.Channels;
using Polly;
using Polly.Extensions.Http;

namespace VideoDecoder;

public class Options
{
    public string Url { get; set; } = string.Empty;              // URL tải m3u8
    public string InputM3U8File { get; set; } = string.Empty;    // Hoặc file m3u8 local
    public string OutputVideoFile { get; set; } = "output.mp4"; // Đầu ra
    public string VideoCodec { get; set; } = "h264_nvenc";      // Khi fallback re-encode
    public int TaskCount { get; set; } = 8;                      // Số tác vụ tải song song
    public int MaxBufferedSegments { get; set; } = 16;           // Giới hạn segment chờ (RAM)
    public bool UseGenPts { get; set; } // -fflags +genpts
    public bool UseCopyTs { get; set; } // -copyts -start_at_zero
    public bool ForceReEncode { get; set; } // Bỏ -c copy, ép mã hóa
}

internal static class Program
{
    // 8 byte: length=0 (00 00 00 00) + "IEND"
    private static readonly byte[] IendLenType = "\0\0\0\0IEND"u8.ToArray();

    private static long _totalBytes;

    static int _completedSegments = 0;
    static readonly object _progressLock = new();

    private static readonly SocketsHttpHandler HttpHandler = new()
    {
        // dev only! Không nên bật trong production
        SslOptions = { RemoteCertificateValidationCallback = (_, _, _, _) => true },
        MaxConnectionsPerServer = 64,
        PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
        KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always,
        KeepAlivePingDelay = TimeSpan.FromSeconds(30),
        KeepAlivePingTimeout = TimeSpan.FromSeconds(10)
    };

    private static readonly HttpClient Http = new(HttpHandler)
    {
        Timeout = TimeSpan.FromSeconds(100)
    };

    private static readonly IAsyncPolicy<HttpResponseMessage> RetryPolicy =
        HttpPolicyExtensions
            .HandleTransientHttpError()
            .OrResult(r => (int)r.StatusCode == 429)
            .WaitAndRetryAsync(
                retryCount: 4,
                sleepDurationProvider: attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
                onRetry: (res, _, attempt, _) =>
                {
                    Console.WriteLine($"Retrying HTTP... Attempt {attempt} ({res.Result?.StatusCode})");
                }
            );

    public static async Task Main(string[] args)
    {
        Console.InputEncoding = Console.OutputEncoding = Encoding.UTF8;
        var options = ParseArguments(args);

        AppDomain.CurrentDomain.ProcessExit += (_, _) => TryDisableCtrlC();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; Cts.Cancel(); };

        var sw = Stopwatch.StartNew();
        Console.WriteLine("Cần ffmpeg có trong PATH.");

        string m3U8Path = await ResolveM3U8Async(options);
        var allLines = await File.ReadAllLinesAsync(m3U8Path);
        var segmentUrls = allLines.Where(x => x.StartsWith("http", StringComparison.OrdinalIgnoreCase)).ToList();
        if (segmentUrls.Count == 0)
        {
            Console.WriteLine("Không tìm thấy segment URL trong m3u8.");
            return;
        }

        // Pipe song song nhưng ghi theo thứ tự, RAM bounded bởi MaxBufferedSegments
        await StreamParallelInOrderAsync(segmentUrls, options);

        sw.Stop();
        Console.WriteLine($"\nHoàn tất trong {sw.Elapsed}. Tổng tải ~ {(_totalBytes / 1024d / 1024d):F2} MB");

        // Dọn m3u8 tạm nếu do chúng ta tải về
        if (!string.IsNullOrEmpty(options.Url))
        {
            try { File.Delete(m3U8Path); } catch { /* ignore */ }
        }
    }

    private static readonly CancellationTokenSource Cts = new();

    private static Options ParseArguments(string[] args)
    {
        var o = new Options();
        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "-h": ShowHelp(); Environment.Exit(0); break;
                case "-i": if (i + 1 < args.Length) o.InputM3U8File = args[++i]; break;
                case "-u": if (i + 1 < args.Length) o.Url = args[++i]; break;
                case "-o": if (i + 1 < args.Length) o.OutputVideoFile = args[++i]; break;
                case "-c": if (i + 1 < args.Length) o.VideoCodec = args[++i]; break;
                case "-t": if (i + 1 < args.Length && int.TryParse(args[++i], out var tc)) o.TaskCount = Math.Max(1, tc); break;
                case "-b": if (i + 1 < args.Length && int.TryParse(args[++i], out var mb)) o.MaxBufferedSegments = Math.Max(1, mb); break;
                case "--genpts": o.UseGenPts = true; break;
                case "--copyts": o.UseCopyTs = true; break;
                case "--reencode": o.ForceReEncode = true; break;
            }
        }

        if (string.IsNullOrEmpty(o.InputM3U8File) && string.IsNullOrEmpty(o.Url))
            throw new ArgumentException("Cần -i <file> hoặc -u <url> cho m3u8");
        if (string.IsNullOrWhiteSpace(o.OutputVideoFile))
            throw new ArgumentException("Thiếu -o <output> file");

        var dir = Path.GetDirectoryName(Path.GetFullPath(o.OutputVideoFile));
        if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
            throw new DirectoryNotFoundException($"Không thấy thư mục: {dir}");

        return o;
    }

    private static void ShowHelp()
    {
        Console.WriteLine("Usage: VideoDecoder [options]");
        Console.WriteLine("  -i <file>        m3u8 local");
        Console.WriteLine("  -u <url>         m3u8 URL");
        Console.WriteLine("  -o <file>        video đầu ra (.mp4/.mkv)");
        Console.WriteLine("  -c <codec>       bộ mã hoá fallback (vd: h264_nvenc)");
        Console.WriteLine("  -t <N>           số tác vụ tải song song (mặc định 8)");
        Console.WriteLine("  -b <N>           số segment tối đa đệm trong RAM (mặc định 16)");
        Console.WriteLine("  --genpts         thêm -fflags +genpts (sinh PTS nếu thiếu)");
        Console.WriteLine("  --copyts         thêm -copyts -start_at_zero (giữ PTS gốc)");
        Console.WriteLine("  --reencode       ép re-encode (bỏ -c copy)");
        Console.WriteLine();
        Console.WriteLine("Ví dụ:");
        Console.WriteLine("  VideoDecoder -u https://.../index.m3u8 -o out.mp4 -t 12 -b 24 --genpts");
    }

    private static async Task<string> ResolveM3U8Async(Options opts)
    {
        if (!string.IsNullOrEmpty(opts.InputM3U8File))
        {
            if (!File.Exists(opts.InputM3U8File))
                throw new FileNotFoundException("Không tìm thấy file m3u8", opts.InputM3U8File);
            return opts.InputM3U8File;
        }

        var temp = Path.Combine(AppContext.BaseDirectory, Path.GetRandomFileName() + ".m3u8");
        using var resp = await RetryPolicy.ExecuteAsync(() => Http.GetAsync(opts.Url));
        resp.EnsureSuccessStatusCode();
        if (resp.Content.Headers.ContentLength is { } cl) Interlocked.Add(ref _totalBytes, cl);
        await using var fs = new FileStream(temp, FileMode.Create, FileAccess.Write, FileShare.None, 64 * 1024, true);
        await resp.Content.CopyToAsync(fs);
        return temp;
    }

    // ===========================
    // PIPE song song nhưng ghi theo thứ tự
    // ===========================

    private record SegBuf(int Index, byte[] Data);

    private static async Task StreamParallelInOrderAsync(IList<string> urls, Options o)
    {
        var chan = Channel.CreateBounded<SegBuf>(new BoundedChannelOptions(o.MaxBufferedSegments)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        });

        using var ff = StartFfmpeg(o);
        await using var ffIn = ff.StandardInput.BaseStream;

        // Consumer: đảm bảo thứ tự ghi 0..N-1
        var consumer = Task.Run(async () =>
        {
            var pending = new SortedDictionary<int, byte[]>();
            int next = 0;

            await foreach (var seg in chan.Reader.ReadAllAsync(Cts.Token))
            {
                pending[seg.Index] = seg.Data;
                while (pending.TryGetValue(next, out var buf))
                {
                    await ffIn.WriteAsync(buf, 0, buf.Length, Cts.Token);
                    pending.Remove(next);
                    next++;
                }
            }

            await ffIn.FlushAsync(Cts.Token);
            try { ff.StandardInput.Close(); } catch { /* ignore */ }
        }, Cts.Token);

        // Producers song song
        RenderSegmentsProgress(0, urls.Count);
        using var sem = new SemaphoreSlim(o.TaskCount);
        var tasks = new List<Task>(urls.Count);
        for (int i = 0; i < urls.Count; i++)
        {
            int idx = i;
            string url = urls[i];

            tasks.Add(Task.Run(async () =>
            {
                await sem.WaitAsync(Cts.Token);
                try
                {
                    using var resp = await RetryPolicy.ExecuteAsync(() =>
                        Http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead, Cts.Token));
                    resp.EnsureSuccessStatusCode();

                    if (resp.Content.Headers.ContentLength is { } cl)
                        Interlocked.Add(ref _totalBytes, cl);

                    await using var inStream = await resp.Content.ReadAsStreamAsync(Cts.Token);
                    using var ms = new MemoryStream(capacity: 256 * 1024);
                    await ExtractTsFromPngStreamAsync(inStream, ms, 64 * 1024, Cts.Token);
                    var data = ms.ToArray();

                    await chan.Writer.WriteAsync(new SegBuf(idx, data), Cts.Token);
                    int done = Interlocked.Increment(ref _completedSegments);
                    RenderSegmentsProgress(done, urls.Count);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Lỗi segment {idx}: {ex.Message}");
                    throw;
                }
                finally
                {
                    sem.Release();
                }
            }, Cts.Token));
        }

        try
        {
            await Task.WhenAll(tasks);
            chan.Writer.Complete();
            await consumer;
            
        }
        finally
        {
            // Kết thúc ffmpeg & kiểm tra mã thoát
            await ff.WaitForExitAsync(Cts.Token);
            if (ff.ExitCode != 0)
                throw new Exception($"ffmpeg exit code = {ff.ExitCode}");

            RenderSegmentsProgress(urls.Count, urls.Count);
        }
    }


    static void RenderSegmentsProgress(int completed, int total)
    {
        double pct = total == 0 ? 0 : (double)completed / total * 100.0;

        // Thanh progress 10..40 ký tự, tuỳ chiều rộng console
        int maxWidth = Console.WindowWidth > 0 ? Console.WindowWidth : 80;
        int barWidth = Math.Clamp(maxWidth - 30, 10, 40);
        int filled = total == 0 ? 0 : (int)Math.Round(barWidth * completed / (double)total);

        string bar = "[" + new string('#', filled) + new string('-', barWidth - filled) + "]";
        string line = $"{bar}  {completed}/{total}  ({pct:0.0}%)";

        lock (_progressLock)
        {
            // Ghi đè cùng 1 dòng
            Console.Write("\r" + line.PadRight(maxWidth - 1));
        }
    }

    private static Process StartFfmpeg(Options o)
    {
        var args = new StringBuilder();
        args.Append("-hide_banner -loglevel warning -y ");
        args.Append("-f mpegts ");
        if (o.UseGenPts) args.Append("-fflags +genpts ");
        if (o.UseCopyTs) args.Append("-copyts -start_at_zero ");
        args.Append("-i pipe:0 ");

        if (o.ForceReEncode)
        {
            args.Append($"-c:v {o.VideoCodec} -c:a copy ");
        }
        else
        {
            args.Append("-c copy ");
        }

        args.Append("-map 0 -max_interleave_delta 0 ");
        if (Path.GetExtension(o.OutputVideoFile).Equals(".mp4", StringComparison.OrdinalIgnoreCase))
            args.Append("-bsf:a aac_adtstoasc -movflags +faststart ");

        args.Append('"').Append(o.OutputVideoFile).Append('"');

        var psi = new ProcessStartInfo
        {
            FileName = "ffmpeg",
            Arguments = args.ToString(),
            UseShellExecute = false,
            RedirectStandardInput = true,
            RedirectStandardError = true,
            RedirectStandardOutput = false,
            StandardInputEncoding = Encoding.UTF8
        };

        var p = new Process { StartInfo = psi, EnableRaisingEvents = true };
        p.Start();

        // hút stderr để tránh nghẽn và in log
        _ = Task.Run(async () =>
        {
            string? line;
            while ((line = await p.StandardError.ReadLineAsync(Cts.Token)) != null)
                Console.WriteLine($"[ffmpeg] {line}");
        }, Cts.Token);

        return p;
    }

    /// <summary>
    /// Tách TS từ PNG stream theo block: tìm mẫu 8 byte (00 00 00 00 'I''E''N''D'), bỏ 4 byte CRC, ghi phần còn lại sang TS.
    /// Không giữ nguyên PNG, không load toàn bộ vào RAM.
    /// </summary>
    private static async Task ExtractTsFromPngStreamAsync(Stream input, Stream tsOutput, int bufferSize, CancellationToken ct)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        try
        {
            int matched = 0;
            int skipCrc = 0;
            bool writing = false;

            while (true)
            {
                int read = await input.ReadAsync(buffer.AsMemory(0, buffer.Length), ct);
                if (read == 0) break;

                int idx = 0;

                if (!writing)
                {
                    while (idx < read && !writing)
                    {
                        byte b = buffer[idx];

                        if (matched < IendLenType.Length)
                        {
                            if (b == IendLenType[matched]) matched++;
                            else matched = (b == IendLenType[0]) ? 1 : 0;
                            idx++;

                            if (matched == IendLenType.Length) skipCrc = 4;
                        }
                        else if (skipCrc > 0)
                        {
                            int canSkip = Math.Min(skipCrc, read - idx);
                            skipCrc -= canSkip;
                            idx += canSkip;

                            if (skipCrc == 0)
                            {
                                writing = true;
                                if (idx < read)
                                {
                                    await tsOutput.WriteAsync(buffer.AsMemory(idx, read - idx), ct);
                                    idx = read;
                                }
                            }
                        }
                    }
                }
                else
                {
                    await tsOutput.WriteAsync(buffer.AsMemory(0, read), ct);
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
    private static void TryDisableCtrlC()
    {
        try { Console.TreatControlCAsInput = true; } catch { /* ignore */ }
    }
}
