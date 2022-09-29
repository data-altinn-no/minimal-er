using System.Diagnostics;
using System.Net;
using Azure.Storage.Blobs;
using ICSharpCode.SharpZipLib.GZip;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace minimal_er;

public class DownloadFromBrregAndProcess
{
    private readonly IConfiguration _configuration;
    private readonly IHttpClientFactory _clientFactory;
    private readonly ILogger _logger;

    public DownloadFromBrregAndProcess(ILoggerFactory loggerFactory, IConfiguration configuration, IHttpClientFactory clientFactory)
    {
        _configuration = configuration;
        _clientFactory = clientFactory;
        _logger = loggerFactory.CreateLogger<DownloadFromBrregAndProcess>();
    }

    [Function("DownloadFromBrregAndProcess")]
    public async Task RunAsyncTimer([TimerTrigger("0 37 2 1,8,15,22 * *"
#if DEBUG
        , RunOnStartup = true
#endif
    )] TimerInfo myTimer)
    {
        _logger.LogInformation($"{nameof(DownloadFromBrregAndProcess)} timer trigger executed at: {DateTime.Now}");
        await PerformDownloadAndProcess();
        _logger.LogInformation($"Next timer schedule for {nameof(DownloadFromBrregAndProcess)} at: {myTimer.ScheduleStatus?.Next}");
    }

    [Function("ForceDownloadFromBrregAndProcess")]
    public async Task<HttpResponseData> RunAsyncHttp([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequestData req)
    {
        _logger.LogInformation($"{nameof(DownloadFromBrregAndProcess)} manual ER refresh executed at: {DateTime.Now}");
            
        await PerformDownloadAndProcess();

        _logger.LogInformation($"{nameof(DownloadFromBrregAndProcess)} manual ER refresh completed at: {DateTime.Now}");

        var response = req.CreateResponse(HttpStatusCode.OK);
        response.Headers.Add("Content-Type", "text/plain; charset=utf-8");
        await response.WriteStringAsync("Done!");
            
        return response;
    }

    private async Task PerformDownloadAndProcess()
    {
        var sw = Stopwatch.StartNew();

        var blobServiceClient = new BlobServiceClient(_configuration["AzureWebJobsStorage"]);
        var containerClient = blobServiceClient.GetBlobContainerClient("minimaler");
        if (!await containerClient.ExistsAsync())
        {
            await containerClient.CreateAsync();
        }
        var client = _clientFactory.CreateClient("brreg");

        var response = await client.GetAsync("https://data.brreg.no/enhetsregisteret/oppslag/enheter/lastned",
            HttpCompletionOption.ResponseHeadersRead);

        _logger.LogInformation("Headers received, starting parse ...");

        var contentStream = await response.Content.ReadAsStreamAsync();
        var cnt = 0;

        var outputStream = new MemoryStream();

        using (var writer = new StreamWriter(outputStream))
        using (var gzipStream = new GZipInputStream(contentStream))
        using (var sr = new StreamReader(gzipStream))
        using (var reader = new JsonTextReader(sr))
        {
            while (reader.Read())
            {
                if (reader.TokenType == JsonToken.StartObject)
                {
                    var entry = JObject.Load(reader).ToObject<JObject>();
                    writer.WriteLine($"{entry!["organisasjonsnummer"]!}\t{entry["navn"]!}");
                }
                cnt++;
#if DEBUG
                if (cnt % 10000 == 0)
                {
                    _logger.LogInformation($"Written {cnt} entries ({(float)cnt/sw.ElapsedMilliseconds*1000} entries/sec)");
                }
#endif
            }

            _logger.LogInformation("Uploading blob ...");
            outputStream.Position = 0;
            var blobClient = containerClient.GetBlobClient("mainunits2.tsv");
            await blobClient.UploadAsync(outputStream, overwrite: true);
            _logger.LogInformation("Upload complete");
        }

        var elapsed = sw.ElapsedMilliseconds;
        var persec = (float)cnt / elapsed * 1000;

        _logger.LogInformation($"Wrote {cnt} entries in {elapsed / 1000} seconds ({persec} entries/sec)");
    }
}