import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, GetCommand } from "@aws-sdk/lib-dynamodb";
import { randomUUID } from "crypto";

const dbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dbClient);

const TABLE_NAME = "HelpRequests"; 

export const handler = async (event) => {
    console.log("Raw Event Data:", JSON.stringify(event, null, 2));

    for (const record of event.Records) {
        try {
            // 1. ข้อความจาก SNS 
            const snsMessage = JSON.parse(record.Sns.Message);
            console.log("Received Event from TrackDonation:", snsMessage);

            if (!snsMessage.referenceReqId) {
                console.log("Skipping event: No referenceReqId found.");
                continue;
            }

            const requestId = snsMessage.referenceReqId; 
            const now = new Date().toISOString();

            // 2. ดึงข้อมูลคำร้องจาก Database
            const getResponse = await docClient.send(new GetCommand({
                TableName: TABLE_NAME,
                Key: { request_id: requestId }
            }));

            let item;

            // Upsert: ถ้าหาไม่เจอ ให้สร้างข้อมูลชุดใหม่
            if (!getResponse.Item) {
                console.log(`HelpRequest ID ${requestId} not found. Creating a new record automatically...`);
                item = {
                    request_id: requestId,
                    incident_id: snsMessage.incidentId || "UNKNOWN_INCIDENT", 
                    request_type: "donation", 
                    created_at: now,
                    current_status: "NEW", // สถานะตั้งต้นก่อนอัปเดต
                    timeline_logs: []      // เตรียม Array ว่างไว้รอรับ Log
                };
            } else {
                item = getResponse.Item;
            }

            // 4. จัดการข้อมูลจาก Event ใหม่ (ดึง title, description, และรายการของ)
            const title = snsMessage.title || "อัปเดตจากคลังสินค้า";
            const description = snsMessage.description || "";
            const detailsStatus = snsMessage.details?.status; // จะเป็น "ALLOCATED" หรือ "CANCELLED"
            const allocatedList = snsMessage.details?.allocatedList || [];

            // สรุปรายการสิ่งของ
            let itemsText = "";
            if (allocatedList.length > 0) {
                itemsText = allocatedList
                    .map(i => `${i.itemName} จำนวน ${i.allocatedAmount}`)
                    .join(", ");
            }

            
            let noteText = `${title}`;
            if (description) noteText += ` - ${description}`;
            if (itemsText) noteText += ` (รายการ: ${itemsText})`;

            // 5. ตรวจสอบเงื่อนไขเพื่ออัปเดตสถานะ DISPATCHED หรือ CANCELLED
            let newStatus = item.current_status; // ค่า default 
            
            if (detailsStatus === "ALLOCATED") {
                newStatus = "DISPATCHED";
            } else if (detailsStatus === "CANCELLED") {
                newStatus = "CANCELLED";
            }

            // 6. บันทึก Timeline
            const newLog = {
                update_id: randomUUID(),
                update_number: item.timeline_logs.length + 1,
                status_at_time: newStatus,
                note: noteText,
                updated_at: snsMessage.timestamp || now,
                updated_by: snsMessage.source || "TrackDonation Service"
            };

            item.timeline_logs.push(newLog);
            item.current_status = newStatus; 
         
            await docClient.send(new PutCommand({
                TableName: TABLE_NAME,
                Item: item
            }));

            console.log(`Successfully updated timeline for ${requestId}`);

        } catch (error) {
            console.error("Error processing record:", error);
            throw error; 
        }
    }
    return "Success";
};