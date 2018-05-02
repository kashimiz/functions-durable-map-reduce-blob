using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace FunctionApp8
{
    public static class Function1
    {
        private static CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("OutputStorage"));
        private static CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

        [FunctionName("Orchestrator")]
        public static async Task<IEnumerable<string>> RunOrchestrator(
            [OrchestrationTrigger] DurableOrchestrationContext context)
        {
            var words = new List<List<string>>();

            IEnumerable<string> textFileNames = await context.CallActivityAsync<IEnumerable<string>>("ListFiles", "out-container");

            List<Task<string>> tasks = new List<Task<string>>();
            foreach (var filename in textFileNames)
            {
                tasks.Add(context.CallActivityAsync<string>("RetrieveContent", filename));
            }

            var completedTasks = await Task.WhenAll(tasks);

            var result = await context.CallActivityAsync<IEnumerable<string>>("CalculateMostUsed", completedTasks);
            return result;
        }

        [FunctionName("ListFiles")]
        public static async Task<IEnumerable<string>> ListFiles([ActivityTrigger] string containerName, TraceWriter log)
        {
            var container = blobClient.GetContainerReference(containerName);
            BlobContinuationToken continuationToken = null;
            List<CloudBlob> results = new List<CloudBlob>();
            do
            {
                var response = await container.ListBlobsSegmentedAsync(continuationToken);
                continuationToken = response.ContinuationToken;
                results.AddRange(response.Results.OfType<CloudBlob>());
            }
            while (continuationToken != null);

            return from t in results select t.Name;
        }

        [FunctionName("RetrieveContent")]
        public static async Task<string> RetrieveContent([ActivityTrigger] string filename, TraceWriter log)
        {
            var container = blobClient.GetContainerReference("out-container");
            using (Stream blobStream = new MemoryStream())
            {
                var blobItem = container.GetBlockBlobReference(filename);
                await blobItem.DownloadToStreamAsync(blobStream);
                blobStream.Position = 0;
                using (StreamReader sr = new StreamReader(blobStream))
                {
                    string content = await sr.ReadToEndAsync();
                    return content;
                }
            }

        }

        [FunctionName("CalculateMostUsed")]
        public static IEnumerable<string> CalculateMostUsed([ActivityTrigger] string[] stickers, TraceWriter log)
        {
            Dictionary<string, int> counts = new Dictionary<string, int>();
            foreach (var sticker in stickers)
            {
                var words = sticker.Split(' ');
                foreach (var word in words)
                {
                    int count;
                    if(counts.TryGetValue(word, out count))
                    {
                        counts[word] = count++;
                    } else
                    {
                        counts[word] = 1;
                    }
                }
            }

            var sortedDict = from w in counts orderby w.Value descending select w.Key;
            return sortedDict.Take(5);
        }

        [FunctionName("Http_StartMapReduce")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")]HttpRequestMessage req,
            [OrchestrationClient]DurableOrchestrationClient starter,
            TraceWriter log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("Orchestrator", null);

            log.Info($"Started orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}