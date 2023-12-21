using Azure;
using Azure.Identity;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using System;
using System.Collections;
using System.ComponentModel;
using System.Text;

namespace Sample01 // Note: actual namespace depends on the project name.
{
    internal static class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("Hello World!");
            //var client = GetBlobServiceClient("viclearningsa");
            //var props = await client.GetPropertiesAsync();

            var serviceClient = GetBlobServiceClientByConnectionString();
            // Create container
            //var container = await CreateSampleContainerAsync(serviceClient);
            var container = serviceClient.GetBlobContainerClient("container-f725d643-def7-4d1b-939e-eb48b76f6259");

            // Add container metadata
            await AddContainerMetadataAsync(container);

            var containerProps = await GetContainerProps(container);
            Console.WriteLine($"Properties for container {container.Uri}");
            Console.WriteLine($"Public access level: {containerProps.PublicAccess}");
            Console.WriteLine($"Last modified time in UTC: {containerProps.LastModified}");

            foreach(var item in containerProps.Metadata )
            {
                Console.WriteLine("Metadata information");
                Console.WriteLine($"Key: {item.Key}, Value: {item.Value}");
            }

        }

        /// <summary>
        /// Can get client but it seems there is no right
        /// </summary>
        /// Refer the series: https://learn.microsoft.com/en-us/dotnet/azure/sdk/authentication/local-development-service-principal?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json&tabs=azure-portal%2Cwindows%2Ccommand-line
        /// <param name="accountName"></param>
        /// <returns></returns>
        public static BlobServiceClient GetBlobServiceClient(string accountName)
        {
            
            BlobServiceClient client = new(
                new Uri($"https://{accountName}.blob.core.windows.net"),
                new DefaultAzureCredential());

            return client;
        }

        /// <summary>
        /// Connect to sa with connection string => have full rights
        /// </summary>
        /// <returns></returns>
        public static BlobServiceClient GetBlobServiceClientByConnectionString()
        {
            string connectionString = "";

            // Create a client that can authenticate with a connection string
            BlobServiceClient service = new BlobServiceClient(connectionString);
            return service;
        }

        /// <summary>
        /// Create container with random name
        /// </summary>
        /// <param name="blobServiceClient"></param>
        /// <returns></returns>
        private static async Task<BlobContainerClient> CreateSampleContainerAsync(BlobServiceClient blobServiceClient)
        {
            // Name the sample container based on new GUID to ensure uniqueness.
            // The container name must be lowercase.
            // Ex: container-f725d643-def7-4d1b-939e-eb48b76f6259
            string containerName = "container-" + Guid.NewGuid();

            try
            {
                // Create the container
                BlobContainerClient container = await blobServiceClient.CreateBlobContainerAsync(containerName);

                if (await container.ExistsAsync())
                {
                    Console.WriteLine("Created container {0}", container.Name);
                    return container;
                }
            }
            catch (RequestFailedException e)
            {
                Console.WriteLine("HTTP error code {0}: {1}", e.Status, e.ErrorCode);
                Console.WriteLine(e.Message);
            }
            return null;

        }

        /// <summary>
        /// Get container props
        /// </summary>
        /// <param name="containerClient"></param>
        /// <returns></returns>
        private static async Task<BlobContainerProperties> GetContainerProps(BlobContainerClient containerClient)
        {
            return await containerClient.GetPropertiesAsync();
        }

        private static async Task AddContainerMetadataAsync(BlobContainerClient containerClient)
        {
            try
            {
                IDictionary<string, string> metadata =
                   new Dictionary<string, string>();

                // Add some metadata to the container.
                metadata.Add("docType", "textDocuments");
                metadata.Add("category", "guidance");

                // Set the container's metadata.
                await containerClient.SetMetadataAsync(metadata);
            }
            catch (RequestFailedException e)
            {
                Console.WriteLine($"HTTP error code {e.Status}: {e.ErrorCode}");
                Console.WriteLine(e.Message);
                Console.ReadLine();
            }
        }

        /// <summary>
        ///  You can use this approach to enhance performance by uploading blocks in parallel.
        /// </summary>
        /// <param name="blobContainerClient"></param>
        /// <param name="localFilePath"></param>
        /// <param name="blockSize"></param>
        /// <returns></returns>
        public static async Task UploadBlocksAsync(BlobContainerClient blobContainerClient,string localFilePath,int blockSize)
        {
            string fileName = Path.GetFileName(localFilePath);
            BlockBlobClient blobClient = blobContainerClient.GetBlockBlobClient(fileName);

            FileStream fileStream = File.OpenRead(localFilePath);
            ArrayList blockIDArrayList = new ArrayList();
            byte[] buffer;

            var bytesLeft = (fileStream.Length - fileStream.Position);

            while (bytesLeft > 0)
            {
                if (bytesLeft >= blockSize)
                {
                    buffer = new byte[blockSize];
                    await fileStream.ReadAsync(buffer, 0, blockSize);
                }
                else
                {
                    buffer = new byte[bytesLeft];
                    await fileStream.ReadAsync(buffer, 0, Convert.ToInt32(bytesLeft));
                    bytesLeft = (fileStream.Length - fileStream.Position);
                }

                using (var stream = new MemoryStream(buffer))
                {
                    string blockID = Convert.ToBase64String(
                        Encoding.UTF8.GetBytes(Guid.NewGuid().ToString()));

                    blockIDArrayList.Add(blockID);
                    await blobClient.StageBlockAsync(blockID, stream);
                }
                bytesLeft = (fileStream.Length - fileStream.Position);
            }

            string[] blockIDArray = (string[])blockIDArrayList.ToArray(typeof(string));

            await blobClient.CommitBlockListAsync(blockIDArray);
        }

        /// <summary>
        /// Upload blob with configuration option
        /// </summary>
        /// <param name="containerClient"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public static async Task UploadBlobWithCustomizationOption(BlobContainerClient containerClient,string blobName)
        {
            BlobClient blobClient = containerClient.GetBlobClient(blobName);
            string blobContents = "Sample blob data";

            // Performance tuning by using StorageTransferOptions 
            var transferOptions = new StorageTransferOptions
            {
                // Set the maximum number of parallel transfer workers
                // The effectiveness of this value is subject to connection pool limits in .NET
                // See: https://devblogs.microsoft.com/azure-sdk/net-framework-connection-pool-limits/
                MaximumConcurrency = 2,

                // Set the initial transfer length to 8 MiB
                // only applies for uploads when using a seekable stream
                // non-seekable stream is ignore
                InitialTransferSize = 8 * 1024 * 1024,

                // Set the maximum length of a transfer to 4 MiB
                MaximumTransferSize = 4 * 1024 * 1024
            };

            // You can specify transfer validation options to help ensure that data is uploaded properly
            // and hasn't been tampered with during transit
            var validationOptions = new UploadTransferValidationOptions
            {
                // Recommended
                ChecksumAlgorithm = StorageChecksumAlgorithm.Auto
            };
            // Index tags, searchable
            IDictionary<string, string> tags = new Dictionary<string, string>
            {
                { "Sealed", "false" },
                { "Content", "image" },
                { "Date", "2020-04-20" }
            };

            BlobUploadOptions options = new BlobUploadOptions
            {
                TransferOptions = transferOptions,
                TransferValidation = validationOptions,
                Tags = tags,
                // Set access tier at blob level
                AccessTier = AccessTier.Hot
            };

            // create stream
            var stream = new MemoryStream();
            var writer = new StreamWriter(stream);
            writer.Write(blobContents);
            writer.Flush();
            stream.Position = 0;
            // Upload from stream to use InitialTransferSize option
            await blobClient.UploadAsync(stream, options);

            // Upload from string
            await blobClient.UploadAsync(BinaryData.FromString(blobContents), options);
        }

        /// <summary>
        /// Download blob with configuration option
        /// </summary>
        /// <param name="blobClient"></param>
        /// <returns></returns>
        public static async Task DownloadBlobWithTransferOptionsAsync(BlobClient blobClient,string localFilePath)
        {
            FileStream fileStream = File.OpenWrite(localFilePath);

            var transferOptions = new StorageTransferOptions
            {
                // Set the maximum number of parallel transfer workers
                MaximumConcurrency = 2,

                // Set the initial transfer length to 8 MiB
                InitialTransferSize = 8 * 1024 * 1024,

                // Set the maximum length of a transfer to 4 MiB
                MaximumTransferSize = 4 * 1024 * 1024
            };

            BlobDownloadToOptions downloadOptions = new BlobDownloadToOptions()
            {
                TransferOptions = transferOptions
            };

            await blobClient.DownloadToAsync(fileStream, downloadOptions);

            fileStream.Close();
        }
    }
   
}