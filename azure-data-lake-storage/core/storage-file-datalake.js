const StorageFileDataLakeLib = require('@azure/storage-file-datalake');

class StorageFileDataLake {
    constructor(storageAccountName, storageAccountKey, fileSystemName) {
        this.storageAccountName = storageAccountName;
        this.storageAccountKey = storageAccountKey;
        this.fileSystemName = fileSystemName;
    }

    async append(fileName, content) {
        const fileSystemClient = this.getFileSystemClient();
        const fileClient = fileSystemClient.getFileClient(fileName);
        const exists = await fileClient.exists();
        if (!exists) {
            await fileClient.create();
        }
        const properties = await fileClient.getProperties();
        await fileClient.append(content, properties.contentLength, content.length);
        const totalSize = properties.contentLength + content.length;
        await fileClient.flush(totalSize);
    }

    createClient() {
        const sharedKeyCredential = new StorageFileDataLakeLib.StorageSharedKeyCredential(
            this.storageAccountName,
            this.storageAccountKey);
        return new StorageFileDataLakeLib.DataLakeServiceClient(
            `https://${this.storageAccountName}.dfs.core.windows.net`,
            sharedKeyCredential);
    }

    getFileSystemClient() {
        const serviceClient = this.createClient();
        return serviceClient.getFileSystemClient(this.fileSystemName);
    }
}

module.exports = {
    StorageFileDataLake: StorageFileDataLake
}