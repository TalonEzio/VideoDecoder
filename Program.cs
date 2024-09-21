using System.Diagnostics;
using System.Text;
using FFMpegCore;

namespace VideoDecoder;

internal class Program
{
    //https://learn.microsoft.com/en-us/dotnet/csharp/language-reference/proposals/csharp-11.0/utf8-string-literals#u8-suffix-on-string-literals
    private static readonly byte[] IendChunk = "IEND"u8.ToArray();

    //or
    //private static readonly byte[] IendChunkOlderVersionBytes = Encoding.UTF8.GetBytes("IEND");
    //private static readonly byte[] IendChunkRawBytes = [0x49, 0x45, 0x4E, 0x44]; // "IEND" in ASCII table

    private static long _totalBytes;

    //private static AsyncLock _asyncLock = new AsyncLock();

    private const int InitialCount = 10; //task concurrency count

    private static async Task Main()
    {

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

        const string url = "https://server.animew.org/videos/97b6245e-f52d-420b-bd0f-27add103f36b/noah_yan2.m3u8";

        const string m3U8FilePath = "sample.m3u8";

        var stopWatch = new Stopwatch();
        stopWatch.Start();

        await DownloadFromUrl(url, m3U8FilePath);

        var content = await File.ReadAllLinesAsync(m3U8FilePath);

        var fakePngUrls = content.Where(x => x.StartsWith("http")).ToList();

        var downloadTasks = new List<Task>();

        var semaphoreSlim = new SemaphoreSlim(InitialCount);




        for (var i = 0; i < fakePngUrls.Count; i++)
        {
            var fakePngUrl = fakePngUrls[i];

            var pngFilePath = Path.Combine(tempDir, $"output{i:D5}.png");

            downloadTasks.Add(PngToTsWrapper(fakePngUrl, pngFilePath, semaphoreSlim));
        }

        await Task.WhenAll(downloadTasks);

        Console.WriteLine($"Dung lượng file:{(_totalBytes / 1024.0 / 1024.0):F2} MB");


        const string outputVideoPath = "output-3.mp4";
        await MergeToFinalVideo(tempDir, outputVideoPath);
        stopWatch.Stop();

        Console.WriteLine($"Xử lý (Download, Merge) hết: {stopWatch}");

        if (Directory.Exists(tempDir))
        {
            foreach (var fileInfo in new DirectoryInfo(tempDir).GetFiles())
            {
                fileInfo.Delete();
            }
        }

        if (File.Exists(m3U8FilePath))
        {
            File.Delete(m3U8FilePath);
        }

    }

    private static async Task MergeToFinalVideo(string inputDirectory, string outputVideoPath)
    {
        var tsFiles = Directory.GetFiles(inputDirectory, "*.ts").OrderBy(x => x).ToList();

        var fileLines = tsFiles.Select(file => $"file '{file.Replace('\\', '/')}").ToList();

        var tempFile = Path.Combine(Path.GetTempPath(), Path.GetTempFileName());

        try
        {
            await File.WriteAllLinesAsync(tempFile, fileLines);

            var ffmpegProcess = FFMpegArguments
                .FromFileInput(tempFile, true, options => { options.WithCustomArgument("-f concat -safe 0"); })
                .OutputToFile(outputVideoPath, true, options =>
                {
                    options.WithCustomArgument("-c:v h264_nvenc"); // h264_nvenc -> livxh264 if u don't have nvidia gpu
                });


            Console.WriteLine(ffmpegProcess.Arguments);

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

        var httpClient = new HttpClient(handler);

        var response = await httpClient.GetAsync(url);

        if (response.Content.Headers.ContentLength.HasValue)
        {
            //Thread-safe
            Interlocked.Add(ref _totalBytes, response.Content.Headers.ContentLength.Value);

            // slower, don't need in here
            //using (await _asyncLock.LockAsync())
            //{
            //    _totalBytes += response.Content.Headers.ContentLength.Value;
            //}


        }
        var localFileStream = new FileStream(outputFilePath, FileMode.OpenOrCreate, FileAccess.Write);

        var httpFileStream = await response.Content.ReadAsStreamAsync();

        await httpFileStream.CopyToAsync(localFileStream);

        await localFileStream.DisposeAsync();

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
                // Trích xuất dữ liệu từ sau IEND
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

    // Hàm tìm vị trí của chunk (vd: IEND) trong mảng byte
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