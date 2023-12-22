using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;


namespace cs722.mapreduce
{
    public static class InvertedIndexMapReduce
    {
        private static readonly HttpClient httpClient = new HttpClient();

        [FunctionName("InvertedIndexOrchestrator")]
        public static async Task<Dictionary<string, List<TermInfo>>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            // Get the list of books
            var bookInfos = context.GetInput<List<BookInfo>>();

            // Prepare to grab the contents of each book
            var fetchBookContentTasks = new List<Task<BookContent>>();

            foreach (var book in bookInfos)
            {
                // Add a task
                fetchBookContentTasks.Add(context.CallActivityAsync<BookContent>("FetchBookContent", book));
            }

            // Wait until all books are fetched
            await Task.WhenAll(fetchBookContentTasks);

            // Prepare to process each book's content
            var mapTasks = new List<Task<Dictionary<string, List<TermInfo>>>>();
            foreach (var bookContent in fetchBookContentTasks.Select(t => t.Result))
            {
                // Add a task to process
                mapTasks.Add(context.CallActivityAsync<Dictionary<string, List<TermInfo>>>("MapWorker", bookContent));
            }

            // Wait until all books are processed
            await Task.WhenAll(mapTasks);

            // Combine the results from all books
            var mapResults = mapTasks.Select(task => task.Result).ToList();
            var combinedResults = await context.CallActivityAsync<Dictionary<string, List<TermInfo>>>("ReduceWorker", mapResults);

            // Format the JSON to be more human readable
            string json = JsonConvert.SerializeObject(combinedResults, Formatting.Indented);
            // Had to use absolute path or output.json would not appear in this directory
            string filePath = @"C:\Users\jlond\project2\output.json";
            System.IO.File.WriteAllText(filePath, json);

            // Return the final combined results
            return combinedResults;
        }

        [FunctionName("FetchBookContent")]
        public static async Task<BookContent> FetchBookContent([ActivityTrigger] BookInfo bookInfo, ILogger log)
        {
            try
            {
                // Try to get the text from the book's URL
                string content = await httpClient.GetStringAsync(bookInfo.Url);
                return new BookContent
                {
                    Name = bookInfo.Name,
                    Content = content
                };
            }
            catch (HttpRequestException e)
            {
                // Error message, log it and return empty content
                log.LogError($"Couldn't fetch book {bookInfo.Name}: {e.Message}");
                return new BookContent
                {
                    Name = bookInfo.Name,
                    Content = "" // Return empty content
                };
            }
        }

        [FunctionName("MapWorker")]
        public static Dictionary<string, List<TermInfo>> MapWorker([ActivityTrigger] BookContent bookContent, ILogger log)
        {
            // Dictionary to keep track of term frequencies for each word
            var termFrequencies = new Dictionary<string, List<TermInfo>>();
           
            // Set of common words to exclude
            var commonWords = new HashSet<string> { "a", "an", "the", "or", "and", "but", "with" };

            // Split the book contents into words
            var words = bookContent.Content.Split(new char[] { ' ', '\r', '\n', '\t' }, StringSplitOptions.RemoveEmptyEntries);
           
            // Define size of bucket & variable to track current bucket
            int bucketSize = 5000;
            int currentBucket = 0;

            // Iterate through each word in book content
            for (int i = 0; i < words.Length; i++)
            {
                // Inc bucket number when the word count > bucket size
                if (i % bucketSize == 0 && i != 0)
                    currentBucket++;
                
                // remove punctuation and make all lower case
                var cleanedWord = new string(words[i].Where(c => !char.IsPunctuation(c)).ToArray()).ToLower();

                if (!commonWords.Contains(cleanedWord))
                {
                    // If the word is not already in the dictionary, create its list of TermInfos
                    if (!termFrequencies.ContainsKey(cleanedWord))
                        termFrequencies[cleanedWord] = new List<TermInfo>();

                    // Finds the TermInfo for the current word in the current bucket, if it exist
                    var termInfo = termFrequencies[cleanedWord].FirstOrDefault(ti => ti.BookName == bookContent.Name && ti.BucketNumber == currentBucket);

                    // create a new a TermInfo object
                    if (termInfo == null)
                    {
                        termInfo = new TermInfo { BookName = bookContent.Name, BucketNumber = currentBucket, TermFrequency = 0 };
                        termFrequencies[cleanedWord].Add(termInfo);
                    }
                    
                    // Incr the term frequency for current word in this bucket
                    termInfo.TermFrequency++;
                }
            }
            // Dictionary containing term frequencies for each word
            return termFrequencies;
        }

        [FunctionName("ReduceWorker")]
        public static Dictionary<string, List<TermInfo>> ReduceWorker(
            [ActivityTrigger] List<Dictionary<string, List<TermInfo>>> mapResults, ILogger log)
        {
            // Empty Dictionary 
            var invertedIndex = new Dictionary<string, List<TermInfo>>();

            // Iterate over each mapResults {term, info}
            foreach (var result in mapResults)
            {
                // Iterate over each termEntry
                foreach (var termEntry in result)
                {
                    var term = termEntry.Key; // word
                    var termInfos = termEntry.Value; // list of TermInfo objects

                    // If the word is not already in the final dictionary, add it with an empty list 
                    if (!invertedIndex.ContainsKey(term))
                        invertedIndex[term] = new List<TermInfo>();

                    // Iterate over each TermInfo object for the current term 
                    foreach (var termInfo in termInfos)
                    {
                        // Check if there is already a TermInfo object for the same book and bucket in the index
                        var existingTermInfo = invertedIndex[term].FirstOrDefault(ti => ti.BookName == termInfo.BookName && ti.BucketNumber == termInfo.BucketNumber);
                        if (existingTermInfo != null)
                            // If it exists, update the term frequency by adding the frequency from the current TermInfo
                            existingTermInfo.TermFrequency += termInfo.TermFrequency;
                        else
                            // Add the new TermInfo object to the list in the index
                            invertedIndex[term].Add(termInfo);
                    }
                }
            }
            // The final inverted index 
            return invertedIndex;
        }

        [FunctionName("InvertedIndex_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Read the list of books from the request
            var bookInfos = await req.Content.ReadAsAsync<List<BookInfo>>();

            string instanceId = await starter.StartNewAsync("InvertedIndexOrchestrator", bookInfos);

            log.LogInformation($"Started Inverted Index orchestration with ID = '{instanceId}'.");

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }

    public class BookInfo
    {
        public string Name { get; set; }
        public string Url { get; set; }
    }

    public class TermInfo
    {
        public string BookName { get; set; }
        public int BucketNumber { get; set; }
        public int TermFrequency { get; set; }
    }

    public class BookContent
    {
        public string Name { get; set; }
        public string Content { get; set; }
    }
}
