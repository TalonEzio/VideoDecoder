using System.Diagnostics;
using System.Text;
using FFMpegCore;
using Polly;
using Polly.Extensions.Http;

namespace VideoDecoder;


public class Options
{
    public string Url { get; set; } = string.Empty;
    public string InputM3U8File { get; set; } = string.Empty;
    public string OutputVideoFile { get; set; } = string.Empty;
    public string VideoCodec { get; set; } = "h264_nvenc";
    public int TaskCount { get; set; } = 1;
}

internal class Program
{
    private static readonly byte[] IendChunk = "IEND"u8.ToArray();

    private static long _totalBytes;

    private static async Task Main(string[] args)
    {
        Console.InputEncoding = Console.OutputEncoding = Encoding.Unicode;
        var options = ParseArguments(args);

        Console.InputEncoding = Console.OutputEncoding = Encoding.Unicode;
        Console.WriteLine("Cần ffmpeg trong Environment, chưa có thì tải");
        var tempDir = Path.Combine(AppContext.BaseDirectory, "Temp");

        if (Directory.Exists(tempDir))
        {
            foreach (var fileInfo in new DirectoryInfo(tempDir).GetFiles())
            {
                fileInfo.Delete();
            }
        }
        else
        {
            Directory.CreateDirectory(tempDir);
        }

        string m3U8FilePath = "demo.m3u8";

        var stopWatch = new Stopwatch();
        stopWatch.Start();

        if (!string.IsNullOrEmpty(options.Url))
        {
            await DownloadFromUrl(options.Url, m3U8FilePath);
        }
        else if (!string.IsNullOrEmpty(options.InputM3U8File))
        {
            m3U8FilePath = options.InputM3U8File;
        }

        var content = await File.ReadAllLinesAsync(m3U8FilePath);

        var fakePngUrls = content.Where(x => x.StartsWith("http")).ToList();

        var downloadTasks = new List<Task>();

        var semaphoreSlim = new SemaphoreSlim(options.TaskCount);

        for (var i = 0; i < fakePngUrls.Count; i++)
        {
            var fakePngUrl = fakePngUrls[i];

            var pngFilePath = Path.Combine(tempDir, $"output{i:D5}.png");

            downloadTasks.Add(PngToTsWrapper(fakePngUrl, pngFilePath, semaphoreSlim));
        }

        await Task.WhenAll(downloadTasks);

        Console.WriteLine($"Dung lượng file:{(_totalBytes / 1024.0 / 1024.0):F2} MB");


        await MergeToFinalVideo(tempDir, options);
        stopWatch.Stop();

        Console.WriteLine($"Xử lý (Download, Merge) hết: {stopWatch}");

        if (Directory.Exists(tempDir))
        {
            foreach (var fileInfo in new DirectoryInfo(tempDir).GetFiles())
            {
                fileInfo.Delete();
            }
        }

        //if (File.Exists(m3U8FilePath))
        //{
        //    File.Delete(m3U8FilePath);
        //}

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
                        {
                            throw new FileNotFoundException("Không tìm thấy file");
                        }
                    }
                    break;
                case "-u":
                    if (i + 1 < args.Length)
                    {
                        options.Url = args[i + 1];
                        if (!options.Url.StartsWith("http"))
                        {
                            throw new ArgumentException("Url không đúng định dạng");
                        }
                    }
                    break;
                case "-o":
                    if (i + 1 < args.Length)
                    {
                        options.OutputVideoFile = args[i + 1];
                        var directoryInfo = new FileInfo(options.OutputVideoFile).Directory;
                        if (directoryInfo is { Exists: false })
                        {
                            throw new ArgumentException("Không thấy thư mục");
                        }
                    }
                    break;
                case "-c":
                    if (i + 1 < args.Length && args[i + 1].Length > 5) options.VideoCodec = args[i + 1];
                    break;
                case "-t":
                    if (i + 1 < args.Length && int.TryParse(args[i + 1], out var taskCount))
                    {
                        options.TaskCount = taskCount;
                    }
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
        Console.WriteLine("  -c <codec>       Bộ mã hóa video, ví dụ: h264_nvenc (mặc định)");
        Console.WriteLine("  -t <count>       Số lượng tác vụ tải xuống đồng thời (mặc định: 10)");
        Console.WriteLine("\nExample:");
        Console.WriteLine("  VideoDecoder -u <url> -o output.mp4 -c h264_nvenc -t 5");
    }

    private static async Task MergeToFinalVideo(string inputDirectory, Options inputOptions)
    {
        var tsFiles = Directory.GetFiles(inputDirectory, "*.ts").OrderBy(x => x).ToList();

        var fileLines = tsFiles.Select(file => $"file '{file.Replace('\\', '/')}").ToList();

        var tempFile = Path.Combine(Path.GetTempPath(), Path.GetTempFileName());

        try
        {
            await File.WriteAllLinesAsync(tempFile, fileLines);

            var ffmpegProcess = FFMpegArguments
                .FromFileInput(tempFile, true, options => { options.WithCustomArgument("-f concat -safe 0"); })
                .OutputToFile(inputOptions.OutputVideoFile, true, options =>
                {
                    options.WithCustomArgument($"-c:v {inputOptions.VideoCodec}"); // h264_nvenc -> livxh264 if u don't have nvidia gpu
                });



            Console.WriteLine($"Arguments: {ffmpegProcess.Arguments}");


            ffmpegProcess.NotifyOnProgress((percent) =>
            {
                Console.Write($"\rTrạng thái: {percent / 10000}%");
            }, TimeSpan.FromSeconds(1));


            await ffmpegProcess.ProcessAsynchronously();

            Console.WriteLine("Gộp ts thành file video hoàn tất!");
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }

        foreach (var tsFile in tsFiles)
        {
            File.Delete(tsFile);
        }

        if (File.Exists(tempFile))
        {
            File.Delete(tempFile);
        }

    }

    private static async Task PngToTsWrapper(string url, string outputPath, SemaphoreSlim semaphoreSlim)
    {
        try
        {
            await semaphoreSlim.WaitAsync();
            await DownloadFromUrl(url, outputPath);

            var tsFilePath = Path.ChangeExtension(outputPath, "ts");

            await ConvertPngToTs(outputPath, tsFilePath);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }
        finally
        {
            semaphoreSlim.Release();
        }
    }

    private static async Task DownloadFromUrl(string url, string outputFilePath)
    {
        var handler = new HttpClientHandler
        {
            ServerCertificateCustomValidationCallback = (message, cert, chain, errors) => true
        };

        var retryPolicy = HttpPolicyExtensions
            .HandleTransientHttpError()
            .WaitAndRetryAsync(
                retryCount: 3, // Số lần retry
                sleepDurationProvider: retryAttempt =>
                    TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), // Thời gian chờ tăng dần
                onRetry: (outcome, timespan, retryAttempt, context) =>
                {
                    Console.WriteLine($"Retrying... Attempt {retryAttempt}");
                });

        var httpClient = new HttpClient(handler);

        try
        {
            var response = await retryPolicy.ExecuteAsync(() => httpClient.GetAsync(url));

#if DEBUG
            Console.WriteLine(response.IsSuccessStatusCode
                ? $"Success download {url}"
                : $"Request failed with status code: {response.StatusCode}");
#endif

            if (response.Content.Headers.ContentLength.HasValue)
            {
                Interlocked.Add(ref _totalBytes, response.Content.Headers.ContentLength.Value);

            }
            var localFileStream = new FileStream(outputFilePath, FileMode.OpenOrCreate, FileAccess.Write);

            var httpFileStream = await response.Content.ReadAsStreamAsync();

            await httpFileStream.CopyToAsync(localFileStream);

            await localFileStream.DisposeAsync();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Request failed: {ex.Message}");
        }
    }


    private static async Task ConvertPngToTs(string pngFilePath, string outputTsFilePath)
    {
        var pngData = await File.ReadAllBytesAsync(pngFilePath);

        var iendIndex = FindChunk(pngData, IendChunk);

        if (iendIndex != -1)
        {
            //end of png file: "IEND" string: 4 bytes + Chunk Type: 4 bytes + CRC: 4 bytes = 12 bytes
            //https://www.w3.org/TR/PNG-Rationale.html#R.Chunk-layout
            var tsDataStartIndex = iendIndex + 12;

            if (tsDataStartIndex < pngData.Length)
            {
                var tsData = new byte[pngData.Length - tsDataStartIndex];
                Array.Copy(pngData, tsDataStartIndex, tsData, 0, tsData.Length);

                await File.WriteAllBytesAsync(outputTsFilePath, tsData);
                Console.WriteLine("Đã lưu segment: " + Path.GetFileName(outputTsFilePath));
            }
            else
            {
                Console.WriteLine("Không có dữ liệu ẩn sau chunk IEND.");
            }
        }
        else
        {
            Console.WriteLine("Không tìm thấy chunk IEND trong file PNG.");
        }
    }

    private static int FindChunk(byte[] data, byte[] chunkName)
    {
        for (var i = 0; i <= data.Length - chunkName.Length; i++)
        {
            var found = !chunkName.Where((t, j) => data[i + j] != t).Any();
            if (found)
            {
                return i;
            }
        }
        return -1;
    }

}