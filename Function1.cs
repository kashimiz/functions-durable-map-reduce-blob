using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;

namespace FunctionApp8
{
    public static class Function1
    {
        private static CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("OutputStorage"));
        private static CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

        [FunctionName("Orchestrator")]
        public static async Task<WordPairs> RunOrchestrator([OrchestrationTrigger] DurableOrchestrationContext context)
        {
            IEnumerable<string> textFileNames = await context.CallActivityAsync<IEnumerable<string>>("ListFiles", "out-container");

            List<Task<string>> tasks = new List<Task<string>>();
            foreach (var filename in textFileNames)
            {
                tasks.Add(context.CallActivityAsync<string>("RetrieveContent", filename));
            }

            var completedTasks = await Task.WhenAll(tasks);

            var result = CalculateMostUsed(completedTasks);
            await context.CallActivityAsync("WriteToBlob", result);

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

        [FunctionName("WriteToBlob")]
        public static async Task WriteToBlobAsJson(
            [ActivityTrigger] DurableActivityContext content,
            [Blob("most-used-words/{rand-guid}.txt", FileAccess.Write, Connection = "OutputStorage")] Stream outBlob,
            TraceWriter log)
        {
            var bytes = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(content.GetInput<WordPairs>()).ToString());
            await outBlob.WriteAsync(bytes, 0, bytes.Length);
        }

        public static WordPairs CalculateMostUsed([ActivityTrigger] string[] stickers)
        {
            Dictionary<string, int> counts = new Dictionary<string, int>();
            foreach (var sticker in stickers)
            {
                var words = sticker.Split(' ');
                foreach (var word in words)
                {
                    int count;
                    if (counts.TryGetValue(word, out count))
                    {
                        counts[word] = ++count;
                    }
                    else
                    {
                        counts[word] = 1;
                    }
                }
            }

            var sortedWords = from w
                             in counts
                              where !string.IsNullOrWhiteSpace(w.Key)
                              orderby w.Value
                              descending select new WordPair { Word = w.Key, Count = w.Value.ToString() };
            return new WordPairs { Words = sortedWords.Take(10) };
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

        public class WordPair
        {
            [JsonProperty("word")]
            public string Word { get; set; }
            [JsonProperty("count")]
            public string Count { get; set; }
        }

        public class WordPairs
        {
            [JsonProperty("words")]
            public IEnumerable<WordPair> Words;
        }
    }
}