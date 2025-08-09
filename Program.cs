using System.Diagnostics;
using System.Text;
using FFMpegCore;
using Polly;
using Polly.Extensions.Http;
using System.Buffers;

namespace VideoDecoder;

public class Options
{
    public string Url { get; set; } = string.Empty;
    public string InputM3U8File { get; set; } = string.Empty;
    public string OutputVideoFile { get; set; } = string.Empty;
    public string VideoCodec { get; set; } = "h264_nvenc";
    public int TaskCount { get; set; } = 4;
}

internal class Program
{
    // 8 byte: length=0 (00 00 00 00) + "IEND"
    private static readonly byte[] IendLenType = "\0\0\0\0IEND"u8.ToArray();
    private static long _totalBytes;

    private static readonly HttpClientHandler HttpHandler = new()
    {
        //dev only!
        ServerCertificateCustomValidationCallback = (_, _, _, _) => true
    };

    private static readonly HttpClient Http = new(HttpHandler)
    {
        Timeout = TimeSpan.FromSeconds(100)
    };

    private static readonly IAsyncPolicy<HttpResponseMessage> RetryPolicy =
        HttpPolicyExtensions
            .HandleTransientHttpError()
            .WaitAndRetryAsync(
                retryCount: 3,
                sleepDurationProvider: attempt => TimeSpan.FromSeconds(Math.Pow(2, attempt)),
                onRetry: (_, _, attempt, _) => Console.WriteLine($"Retrying... Attempt {attempt}")
            );

    private static async Task Main(string[] args)
    {
        Console.InputEncoding = Console.OutputEncoding = Encoding.Unicode;
        var options = ParseArguments(args);

        Console.WriteLine("Cần ffmpeg trong Environment, chưa có thì tải.");
        var tempDir = Path.Combine(AppContext.BaseDirectory, "Temp");

        if (Directory.Exists(tempDir))
        {
            foreach (var fileInfo in new DirectoryInfo(tempDir).GetFiles())
                fileInfo.Delete();
        }
        else
        {
            Directory.CreateDirectory(tempDir);
        }

        string outputM3U8FilePath = "demo.m3u8";

        var stopWatch = Stopwatch.StartNew();

        if (!string.IsNullOrEmpty(options.Url))
        {
            await DownloadFile(options.Url, outputM3U8FilePath);
        }
        else if (!string.IsNullOrEmpty(options.InputM3U8File))
        {
            outputM3U8FilePath = options.InputM3U8File;
        }

        var content = await File.ReadAllLinesAsync(outputM3U8FilePath);
        var segmentUrls = content.Where(x => x.StartsWith("http", StringComparison.OrdinalIgnoreCase)).ToList();

        var semaphore = new SemaphoreSlim(options.TaskCount);
        var tasks = new List<Task>(segmentUrls.Count);

        for (int i = 0; i < segmentUrls.Count; i++)
        {
            string url = segmentUrls[i];
            string tsPath = Path.Combine(tempDir, $"output{i:D5}.ts");
            tasks.Add(DownloadAndExtractTsWrapper(url, tsPath, semaphore));
        }

        await Task.WhenAll(tasks);

        Console.WriteLine($"\nDung lượng file tải: {(_totalBytes / 1024.0 / 1024.0):F2} MB");

        await MergeToFinalVideo(tempDir, options);

        stopWatch.Stop();
        Console.WriteLine($"Xử lý (Download, Merge) hết: {stopWatch}");

        if (Directory.Exists(tempDir))
        {
            foreach (var fileInfo in new DirectoryInfo(tempDir).GetFiles())
                fileInfo.Delete();
        }
    }

    private static Options ParseArguments(string[] args)
    {
        var options = new Options();

        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "-h":
                    ShowHelp();
                    Environment.Exit(0);
                    break;
                case "-i":
                    if (i + 1 < args.Length)
                    {
                        options.InputM3U8File = args[i + 1];
                        if (!File.Exists(options.InputM3U8File))
                            throw new FileNotFoundException("Không tìm thấy file");
                    }
                    break;
                case "-u":
                    if (i + 1 < args.Length)
                    {
                        options.Url = args[i + 1];
                        if (!options.Url.StartsWith("http", StringComparison.OrdinalIgnoreCase))
                            throw new ArgumentException("Url không đúng định dạng");
                    }
                    break;
                case "-o":
                    if (i + 1 < args.Length)
                    {
                        options.OutputVideoFile = args[i + 1];
                        var directoryInfo = new FileInfo(options.OutputVideoFile).Directory;
                        if (directoryInfo is { Exists: false })
                            throw new ArgumentException("Không thấy thư mục");
                    }
                    break;
                case "-c":
                    if (i + 1 < args.Length && args[i + 1].Length > 5)
                        options.VideoCodec = args[i + 1];
                    break;
                case "-t":
                    if (i + 1 < args.Length && int.TryParse(args[i + 1], out var taskCount))
                        options.TaskCount = Math.Max(1, taskCount);
                    break;
            }
        }
        return options;
    }

    private static void ShowHelp()
    {
        Console.WriteLine("Usage: VideoDecoder [options]");
        Console.WriteLine("Options:");
        Console.WriteLine("  -h               Hiển thị trợ giúp");
        Console.WriteLine("  -i <file>        Đường dẫn đến tệp M3U8 đầu vào");
        Console.WriteLine("  -u <url>         URL để tải xuống tệp M3U8");
        Console.WriteLine("  -o <file>        Đường dẫn đến tệp video đầu ra");
        Console.WriteLine("  -c <codec>       Bộ mã hóa video khi fallback, ví dụ: h264_nvenc (mặc định)");
        Console.WriteLine("  -t <count>       Số lượng tác vụ tải xuống đồng thời (mặc định: 4)");
        Console.WriteLine("\nExample:");
        Console.WriteLine("  VideoDecoder -u <url> -o output.mp4 -c h264_nvenc -t 5");
    }

    // ===========================
    // Download + Extract (stream)
    // ===========================

    private static async Task DownloadAndExtractTsWrapper(string url, string tsOutputPath, SemaphoreSlim semaphore)
    {
        try
        {
            await semaphore.WaitAsync();
            await DownloadAndExtractTsAsync(url, tsOutputPath);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }
        finally
        {
            semaphore.Release();
        }
    }

    private static async Task DownloadAndExtractTsAsync(string url, string tsOutputPath, int bufferSize = 64 * 1024)
    {
        using var resp = await RetryPolicy.ExecuteAsync(() =>
            Http.GetAsync(url, HttpCompletionOption.ResponseHeadersRead));

        if (!resp.IsSuccessStatusCode)
        {
            Console.WriteLine($"Request failed with status code: {resp.StatusCode}");
            resp.EnsureSuccessStatusCode();
        }

        if (resp.Content.Headers.ContentLength.HasValue)
            Interlocked.Add(ref _totalBytes, resp.Content.Headers.ContentLength.Value);

        await using var inStream = await resp.Content.ReadAsStreamAsync();
        await using var outStream = new FileStream(tsOutputPath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize, useAsync: true);

        await ExtractTsFromPngStreamAsync(inStream, outStream, bufferSize);
        Console.WriteLine($"Đã lưu segment: {Path.GetFileName(tsOutputPath)}");
    }

    /// <summary>
    /// Đọc stream PNG theo block, tìm mẫu 8 byte (00 00 00 00 'I' 'E' 'N' 'D'), bỏ 4 byte CRC, rồi ghi phần còn lại sang TS.
    /// Không giữ nguyên file PNG, không load toàn bộ vào RAM.
    /// </summary>
    private static async Task ExtractTsFromPngStreamAsync(Stream input, Stream tsOutput, int bufferSize)
    {
        byte[] buffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        try
        {
            int matched = 0;
            int skipCrc = 0;
            bool writing = false;

            while (true)
            {
                int read = await input.ReadAsync(buffer.AsMemory(0, buffer.Length));
                if (read == 0) break;

                int idx = 0;

                if (!writing)
                {
                    while (idx < read && !writing)
                    {
                        byte b = buffer[idx];

                        if (matched < IendLenType.Length)
                        {
                            if (b == IendLenType[matched])
                            {
                                matched++;
                            }
                            else
                            {
                                matched = (b == IendLenType[0]) ? 1 : 0;
                            }
                            idx++;

                            if (matched == IendLenType.Length)
                            {
                                skipCrc = 4;
                            }
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
                                    await tsOutput.WriteAsync(buffer.AsMemory(idx, read - idx));
                                    idx = read;
                                }
                            }
                        }
                    }
                }
                else
                {
                    await tsOutput.WriteAsync(buffer.AsMemory(0, read));
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
    private static async Task MergeToFinalVideo(string inputDirectory, Options inputOptions)
    {
        var tsFiles = Directory.GetFiles(inputDirectory, "*.ts").OrderBy(x => x).ToList();
        if (tsFiles.Count == 0)
        {
            Console.WriteLine("Không có file .ts để gộp.");
            return;
        }

        var fileLines = tsFiles.Select(file => $"file '{file.Replace('\\', '/')}'").ToList();
        var tempList = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName() + ".txt");

        try
        {
            await File.WriteAllLinesAsync(tempList, fileLines, new UTF8Encoding(false));

            var copyProc = FFMpegArguments
                .FromFileInput(tempList, true, o => o.WithCustomArgument("-f concat -safe 0"))
                .OutputToFile(inputOptions.OutputVideoFile, true, o =>
                {
                    o.WithCustomArgument("-c copy -map 0 -max_interleave_delta 0");
                    if (Path.GetExtension(inputOptions.OutputVideoFile).Equals(".mp4", StringComparison.OrdinalIgnoreCase))
                        o.WithCustomArgument("-bsf:a aac_adtstoasc -movflags +faststart");
                });

            Console.WriteLine($"Arguments (try copy): {copyProc.Arguments}");
            copyProc.NotifyOnProgress(p => Console.Write($"\rTrạng thái (copy): {p / 10000}%"), TimeSpan.FromSeconds(1));

            try
            {
                await copyProc.ProcessAsynchronously();
                Console.WriteLine("\nGộp (copy) hoàn tất!");
            }
            catch (Exception exCopy)
            {
                Console.WriteLine($"\nCopy fail, fallback re-encode: {exCopy.Message}");

                var reencProc = FFMpegArguments
                    .FromFileInput(tempList, true, o => o.WithCustomArgument("-f concat -safe 0"))
                    .OutputToFile(inputOptions.OutputVideoFile, true, o =>
                    {
                        o.WithCustomArgument($"-c:v {inputOptions.VideoCodec} -c:a copy -movflags +faststart -map 0");
                        o.WithCustomArgument("-preset p1");
                    });

                Console.WriteLine($"Arguments (re-encode): {reencProc.Arguments}");
                reencProc.NotifyOnProgress(p => Console.Write($"\rTrạng thái (re-encode): {p / 10000}%"), TimeSpan.FromSeconds(1));
                await reencProc.ProcessAsynchronously();

                Console.WriteLine("\nGộp (re-encode) hoàn tất!");
            }
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }
        finally
        {
            foreach (var tsFile in tsFiles)
            {
                try { File.Delete(tsFile); } catch { /* ignore */ }
            }

            if (File.Exists(tempList))
            {
                try { File.Delete(tempList); } catch { /* ignore */ }
            }
        }
    }

    // ===========================
    // Download helper (M3U8 file)
    // ===========================

    private static async Task DownloadFile(string url, string outputFilePath)
    {
        using var resp = await RetryPolicy.ExecuteAsync(() => Http.GetAsync(url));
        if (!resp.IsSuccessStatusCode)
        {
            Console.WriteLine($"Request failed with status code: {resp.StatusCode}");
            resp.EnsureSuccessStatusCode();
        }

        if (resp.Content.Headers.ContentLength.HasValue)
            Interlocked.Add(ref _totalBytes, resp.Content.Headers.ContentLength.Value);

        await using var local = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.None, 64 * 1024, true);
        await using var httpStream = await resp.Content.ReadAsStreamAsync();
        await httpStream.CopyToAsync(local);
    }
}
