import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, GetCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { randomUUID } from "crypto";

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);
const TABLE_NAME = "HelpRequests";

// =====================================================================
// ฟังก์ชันจำลอง Asynchronous Workflow (Event Producer)
// =====================================================================
async function mockPublishAsyncEvent(eventName, payload) {
    console.log(`[ASYNC WORKFLOW] 📢 Publishing Event: "${eventName}" to Message Broker...`);
    console.log(`[ASYNC WORKFLOW] 📦 Payload Data:`, JSON.stringify(payload));
}

export const handler = async (event) => {
    console.log("Incoming Event:", JSON.stringify(event, null, 2));

    try {
        const routeKey = event.routeKey;

        // ---------------------------------------------------------
        // 1. POST /v1/help-requests (สร้างคำร้องใหม่) - Sync + Async
        // ---------------------------------------------------------
        if (routeKey === "POST /v1/help-requests") {
            const body = JSON.parse(event.body);
            const requestId = `REQ-${randomUUID().substring(0, 8).toUpperCase()}`;
            const now = new Date().toISOString();

            const newItem = {
                request_id: requestId,
                incident_id: body.incident_id, 
                reporter_id: body.reporter_id, 
                location_id: body.location_id,
                request_type: body.request_type,
                description: body.description,
                current_status: "NEW", 
                created_at: now,
                timeline_logs: [{
                    update_id: randomUUID(),
                    update_number: 1,
                    status_at_time: "NEW",
                    note: "Request created",
                    updated_at: now,
                    updated_by: "System"
                }]
            };

            await docClient.send(new PutCommand({
                TableName: TABLE_NAME,
                Item: newItem
            }));

            // ยิง Event แจ้งเตือน (Asynchronous Fire-and-Forget)
            mockPublishAsyncEvent("HelpRequestStatusUpdated", {
                event_type: "HelpRequestStatusUpdated",
                request_id: requestId,
                incident_id: body.incident_id,
                current_status: "NEW",
                timestamp: now
            });

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
        // 2. GET /v1/help-requests/{request_id}/timeline (ดูประวัติ) - Sync
        // ---------------------------------------------------------
        if (routeKey === "GET /v1/help-requests/{request_id}/timeline") {
            const requestId = event.pathParameters.request_id;
            const response = await docClient.send(new GetCommand({
                TableName: TABLE_NAME,
                Key: { request_id: requestId }
            }));

            if (!response.Item) return { statusCode: 404, body: JSON.stringify({ message: "Not found" }) };

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

        // ---------------------------------------------------------
        // 3. PATCH /v1/help-requests/{request_id}/status (อัปเดตสถานะ) - Sync + Async
        // ---------------------------------------------------------
        if (routeKey === "PATCH /v1/help-requests/{request_id}/status") {
            const requestId = event.pathParameters.request_id;
            const body = JSON.parse(event.body);
            const now = new Date().toISOString();

            const getResponse = await docClient.send(new GetCommand({
                TableName: TABLE_NAME,
                Key: { request_id: requestId }
            }));

            if (!getResponse.Item) return { statusCode: 404, body: JSON.stringify({ message: "Not found" }) };

            const item = getResponse.Item;
            const previousStatus = item.current_status;
            const newUpdateNumber = item.timeline_logs.length + 1;

            const newLog = {
                update_id: randomUUID(),
                update_number: newUpdateNumber,
                status_at_time: body.new_status,
                note: body.note,
                updated_at: now,
                updated_by: "Dispatcher-01"
            };

            item.timeline_logs.push(newLog);
            item.current_status = body.new_status;

            await docClient.send(new PutCommand({
                TableName: TABLE_NAME,
                Item: item
            }));

            // ยิง Event เมื่อสถานะเปลี่ยน (Async)
            mockPublishAsyncEvent("HelpRequestStatusUpdated", {
                event_type: "HelpRequestStatusUpdated",
                request_id: requestId,
                incident_id: item.incident_id,
                previous_status: previousStatus,
                current_status: item.current_status,
                timestamp: now
            });

            return {
                statusCode: 200,
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    request_id: item.request_id,
                    current_status: item.current_status,
                    update_number: newUpdateNumber,
                    updated_at: now
                })
            };
        }

        // ---------------------------------------------------------
        // 4. GET /v1/help-requests (ค้นหาด้วย incident_id) - Sync
        // ---------------------------------------------------------
        if (routeKey === "GET /v1/help-requests") {
            const incidentId = event.queryStringParameters?.incident_id;
            
            if (!incidentId) {
                return { 
                    statusCode: 400, 
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ error: "Missing incident_id query parameter" }) 
                };
            }

            // ค้นหาข้อมูลในตารางทั้งหมดที่ตรงกับ incident_id ที่ส่งมา
            const response = await docClient.send(new ScanCommand({
                TableName: TABLE_NAME,
                FilterExpression: "incident_id = :incId",
                ExpressionAttributeValues: {
                    ":incId": incidentId
                }
            }));

            // จัดรูปแบบข้อมูลให้ตรงตาม API Contract
            const formattedData = response.Items.map(item => ({
                request_id: item.request_id,
                reporter_id: item.reporter_id,
                request_type: item.request_type,
                current_status: item.current_status,
                created_at: item.created_at
            }));

            return {
                statusCode: 200,
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    incident_id: incidentId,
                    total_requests: formattedData.length,
                    data: formattedData
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