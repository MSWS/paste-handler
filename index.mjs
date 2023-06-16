import { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import crypto from "crypto";

let s3Client = new S3Client({ region: process.env.AWS_REGION });

export const handler = async (event) => {
    let httpMethod = event.requestContext.http.method;
    let bucketName = process.env.BUCKET_NAME;
    console.log(event);

    if (httpMethod === "GET") {
        // Get last /[key] segment of path
        let objectKey = event.rawPath.split("/").pop();

        try {
            const command = new GetObjectCommand({
                Bucket: bucketName,
                Key: "pastes/" + objectKey,
            });
            const response = await s3Client.send(command);
            return {
                statusCode: 200,
                body: await streamToString(response.Body),
                headers: {
                    "Content-Type": "application/json",
                },
            };
        } catch (err) {
            console.error(err);
            return {
                statusCode: 500,
                body: JSON.stringify({ error: err.message }),
            };
        }
    } else if (httpMethod === "POST") {
        let objectContent = event.body;
        let sha256Hash = crypto.createHash('sha256').update(objectContent).digest('base64url').toString();
        let objectKey = sha256Hash.substring(0, 8);

        try {
            while (await objectExists(bucketName, "pastes/" + objectKey)) {
                const existingObjectContent = await getObjectContent(bucketName, "pastes/" + objectKey);
                if (existingObjectContent === objectContent) {
                    return {
                        statusCode: 200,
                        body: JSON.stringify({ message: "duplicate", key: objectKey }),
                    };
                }
                objectKey = bumpBase64(objectKey);
            }

            const command = new PutObjectCommand({
                Bucket: bucketName,
                Key: "pastes/" + objectKey,
                Body: objectContent,
            });
            await s3Client.send(command);
            return {
                statusCode: 200,
                body: JSON.stringify({ key: objectKey }),
            };
        } catch (err) {
            console.error(err);
            return {
                statusCode: 500,
                body: JSON.stringify({ error: err.message }),
            };
        }
    } else {
        return {
            statusCode: 400,
            body: JSON.stringify({ error: "Invalid HTTP method" }),
        };
    }
}

async function objectExists(bucket, key) {
    try {
        const command = new HeadObjectCommand({ Bucket: bucket, Key: key });
        const data = await s3Client.send(command);
        return data.$metadata.httpStatusCode === 200;
    } catch (err) {
        if (err.$metadata?.httpStatusCode === 404) {
            // doesn't exist and permission policy includes s3:ListBucket
            return false;
        } else if (err.$metadata?.httpStatusCode === 403) {
            // doesn't exist, permission policy WITHOUT s3:ListBucket
            return false;
        } else {
            throw err;
        }
    }
}

async function getObjectContent(bucket, key) {
    const command = new GetObjectCommand({ Bucket: bucket, Key: key });
    const response = await s3Client.send(command);
    return await streamToString(response.Body);
}

function streamToString(stream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("error", reject);
        stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    });
}

const base64Chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
function bumpBase64(str) {
    const lastChar = str.charAt(str.length - 1);
    const lastCharIndex = base64Chars.indexOf(lastChar);
    if (lastCharIndex === -1) {
        // If the last character is not a valid base64 character return the original string.
        return str;
    }
    if (lastCharIndex === base64Chars.length - 1) {
        // If the last character is the last base64 character, replace it with the first base64 character.
        return str + base64Chars.charAt(0);
    }
    // Otherwise, replace the last character with the next base64 character.
    return str.slice(0, -1) + base64Chars.charAt(lastCharIndex + 1);
}