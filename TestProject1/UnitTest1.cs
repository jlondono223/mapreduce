using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using Xunit;
using cs722.mapreduce;



namespace cs722.mapreduce.Tests
{
    public class MapWorkerTests
    {
       [Fact]
        public void TestMapWorkerWithSampleDocuments()
        {
            // Arrange: Create sample BookInfo objects
            var BookInfos = new List<BookInfo>
            {
                new BookInfo
                {
                    Name = "Dracula",
                    Url = "https://www.gutenberg.org/cache/epub/345/pg345.txt",
                },
                new BookInfo
                {
                    Name = "Napoleon's Letter to Josephine",
                    Url = "https://www.gutenberg.org/cache/epub/37499/pg37499.txt",
                },
                new BookInfo
                {
                    Name = "The Time Machine",
                    Url = "https://www.gutenberg.org/cache/epub/35/pg35.txt",
                }
            };

            ///////
        }
    }
}