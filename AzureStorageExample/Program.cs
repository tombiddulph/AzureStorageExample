using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureStorageExample
{
    class Program
    {
        private static CloudStorageAccount _storageAccount;
        private static CloudTableClient _client;
        private static CloudTable _table;
        private const string TableName = "Test";
        private static readonly Random Random = new Random();

        static async Task Main(string[] args)
        {
            await Init();

            await AddEntities();


        


            Console.WriteLine($"There are {_table.ExecuteQuerySegmentedAsync(new TableQuery<SampleEntity>(), new TableContinuationToken()).Result.Count()} entities in {_table.Name}");
            Console.WriteLine("Hello World!");
            Console.Read();
        }

        private static async Task Init()
        {
            _storageAccount = CloudStorageAccount.DevelopmentStorageAccount;

            if (_storageAccount == null)
            {
                Console.WriteLine("Couldn't locate storage account");
                Console.Read();
                return;
            }

            _client = _storageAccount.CreateCloudTableClient();

            _table = _client.GetTableReference(TableName);

            await _table.CreateIfNotExistsAsync();
        }

        private static async Task AddEntities(int count = 99)
        {

            if (count > 100)
            {
                count = 100;
            }



            for (int i = 0; i < count; ++i)
            {

                await _table.ExecuteAsync(TableOperation.InsertOrMerge(new SampleEntity
                {
                    Names = new List<string>
                        {
                            $"Test{Random.Next(0, 500)}",
                            $"Test2{Random.Next(0, 500)}"
                        },
                    NextDate = DateTime.Today.AddDays(Random.Next(-10000, 10000)),

                }));
            }


        }
    }

    class SampleEntity : TableEntity
    {

        public SampleEntity()
        {
            this.PartitionKey = "Sample";
            this.RowKey = Guid.NewGuid().ToString();

            this.Names = new List<string>();
        }

        public string NamesList { get; set; }

        [IgnoreProperty]
        public List<string> Names { get; set; }

        public DateTime NextDate { get; set; }

        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            if (this.Names.Any())
            {
                this.NamesList = string.Join(';', this.Names);
            }

            return base.WriteEntity(operationContext);
        }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);
            if (!string.IsNullOrEmpty(this.NamesList))
            {
                this.Names = this.NamesList.Split(';').ToList();
            }
        }
    }
}
