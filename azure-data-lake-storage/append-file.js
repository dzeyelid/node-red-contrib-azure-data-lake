const storageFileDataLakeLib = require('./core/storage-file-datalake');
const StorageFileDataLake = storageFileDataLakeLib.StorageFileDataLake;

module.exports = function(RED) {
    function AzureDataLakeAppendFileNode(config) {
        RED.nodes.createNode(this, config);

        const storageAccountName = this.credentials.account;
        const storageAccountKey = this.credentials.key;
        const fileSystemName = this.credentials.fileSystemName;

        const node = this;
        node.on('input', async function(msg, send, done) {
            let errors = [];
            if (!msg.payload.hasOwnProperty('filePath')) {
                errors.push('`filePath` field is required in `msg.payload`.');
            }
            if (!msg.payload.hasOwnProperty('content')) {
                errors.push('`content` field is required in `msg.payload`.');
            }

            if (errors.length) {
                errors.forEach((error) => {
                    if (done) {
                        done(error);
                    }
                });
                return;
            }

            const filePath = msg.payload.filePath;
            const content = msg.payload.content;

            try {
                this.status({fill: "blue", shape: "dot", text: "Excuting"});

                const storageFileDataLake = new StorageFileDataLake(
                    storageAccountName,
                    storageAccountKey,
                    fileSystemName,
                );
                await storageFileDataLake.append(filePath, content);

                this.status({fill: "green", shape: "dot", text: "Appended"});

                msg.payload = {
                    filePath: filePath,
                    appendedContent: content,
                };
                node.send(msg);
            } catch (e) {
                this.status({fill: "red", shape: "dot", text: "Failed"});
                if (done) {
                    done(e);
                    return;
                }
            }
        });
    }
    RED.nodes.registerType("append-file", AzureDataLakeAppendFileNode, {
        credentials: {
            account: {
                type: "text",
                required: true,
            },
            key: {
                type: "password",
                required: true,
            },
            fileSystemName: {
                type: "text",
                required: true,
            },
        }
    });
}