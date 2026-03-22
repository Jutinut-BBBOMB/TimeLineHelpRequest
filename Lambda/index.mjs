import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, GetCommand } from "@aws-sdk/lib-dynamodb";
import { randomUUID } from "crypto";

// สร้างตัวเชื่อมต่อกับ DynamoDB
const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);
const TABLE_NAME = "HelpRequests";

// =====================================================================
// ฟังก์ชันจำลอง Asynchronous (Event Producer)
// =====================================================================
async function mockPublishAsyncEvent(eventName, payload) {
    // Dummy
    console.log(`[ASYNC WORKFLOW] Publishing Event: "${eventName}" to Message Broker`);
    // 
    console.log(`[ASYNC WORKFLOW] Payload Data:`, JSON.stringify(payload));
}

export const handler = async (event) => {
    console.log("Incoming Event:", JSON.stringify(event, null, 2));

    try {
        // ดึง Route ที่ API Gateway ส่งมา
        const routeKey = event.routeKey;

        // ---------------------------------------------------------
        // 1. POST /v1/help-requests (สร้างคำร้องใหม่)
        // ---------------------------------------------------------
        if (routeKey === "POST /v1/help-requests") {
            const body = JSON.parse(event.body);
            const requestId = `REQ-${randomUUID().substring(0, 8).toUpperCase()}`;
            const now = new Date().toISOString();

            // จัดเตรียมข้อมูลตาม Contract
            const newItem = {
                request_id: requestId,
                incident_id: body.incident_id, 
                reporter_id: body.reporter_id, 
                location_id: body.location_id,
                request_type: body.request_type,
                description: body.description,
                current_status: "NEW", 
                created_at: now,
                timeline_logs: [ // สร้าง Log แรกสุดลงใน Array ทันที
                    {
                        update_id: randomUUID(),
                        update_number: 1,
                        status_at_time: "NEW",
                        note: "Request created", 
                        updated_at: now,
                        updated_by: "System"
                    }
                ]
            };

            // บันทึกลง DynamoDB
            await docClient.send(new PutCommand({
                TableName: TABLE_NAME,
                Item: newItem
            }));
            // ยิง Event แจ้งเตือน (Asynchronous)
            mockPublishAsyncEvent("HelpRequestCreatedEvent", newItem);

            // ส่ง Response กลับไป
            return {
                statusCode: 201,
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    request_id: requestId,
                    current_status: "NEW", 
                    created_at: now 
                })
            };
        }

        // ---------------------------------------------------------
        // 2. GET /v1/help-requests/{request_id}/timeline (ดูประวัติ)
        // ---------------------------------------------------------
        if (routeKey === "GET /v1/help-requests/{request_id}/timeline") {
            const requestId = event.pathParameters.request_id; // ดึง ID จาก URL

            // ค้นหาข้อมูลจาก DynamoDB
            const response = await docClient.send(new GetCommand({
                TableName: TABLE_NAME,
                Key: { request_id: requestId }
            }));

            if (!response.Item) {
                return { 
                    statusCode: 404, 
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ message: "Help request not found" }) 
                };
            }

            // ส่ง Response คืนเฉพาะฟิลด์ที่ต้องการตาม Contract
            return {
                statusCode: 200,
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    request_id: response.Item.request_id,
                    current_status: response.Item.current_status, 
                    timeline_logs: response.Item.timeline_logs 
                })
            };
        }



        // กรณีเรียก Route ผิด
        return { statusCode: 404, body: JSON.stringify({ message: "Route not found" }) };

    } catch (error) {
        console.error("Error processing request:", error);
        return {
            statusCode: 500,
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ message: "Internal Server Error", error: error.message })
        };
    }
};