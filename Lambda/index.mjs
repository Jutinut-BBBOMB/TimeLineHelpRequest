import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, PutCommand, GetCommand, ScanCommand } from "@aws-sdk/lib-dynamodb";
import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";
import { randomUUID } from "crypto";

const dbClient = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(dbClient);
const snsClient = new SNSClient({});

const TABLE_NAME = "HelpRequests";
const SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:559571211561:HelpRequestEventsTopic"; 
const FRIEND_INCIDENT_API_URL = "https://dcvvgbft6j.execute-api.us-east-1.amazonaws.com/v1/incidents"; 

export const handler = async (event) => {
    console.log("Incoming Event:", JSON.stringify(event, null, 2));

    try {
        const routeKey = event.routeKey;

        // ---------------------------------------------------------
        // 1. POST /v1/help-requests
        // ---------------------------------------------------------
        if (routeKey === "POST /v1/help-requests") {
            const body = JSON.parse(event.body);
            const traceId = event.headers?.['x-trace-id'] || `trace-${randomUUID()}`;
            const requestId = `REQ-${randomUUID().substring(0, 8).toUpperCase()}`;
            const now = new Date().toISOString();

            let isIncidentVerified = true;
            console.log(`[${traceId}] Checking incident_id: ${body.incident_id}...`);
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 3000);

            try {
                const response = await fetch(`${FRIEND_INCIDENT_API_URL}/${body.incident_id}`, { 
                    method: 'GET',
                    headers: { 'Content-Type': 'application/json', 'X-Trace-Id': traceId },
                    signal: controller.signal 
                });
                clearTimeout(timeoutId);
                if (response.status === 200) {
                    isIncidentVerified = true;
                } else {
                    isIncidentVerified = false;
                }
            } catch (error) {
                clearTimeout(timeoutId);
                isIncidentVerified = false;
            }

            const newItem = {
                request_id: requestId,
                incident_id: body.incident_id,
                incident_verified: isIncidentVerified, 
                reporter_id: body.reporter_id,
                request_type: body.request_type,
                description: body.description,
                current_status: isIncidentVerified ? "NEW" : "PENDING_VERIFICATION", 
                created_at: now,
                timeline_logs: [{
                    update_id: randomUUID(),
                    update_number: 1,
                    status_at_time: isIncidentVerified ? "NEW" : "PENDING_VERIFICATION",
                    note: isIncidentVerified ? "Request created" : "Request created (System offline - incident unverified)",
                    updated_at: now,
                    updated_by: "System"
                }]
            };

            await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: newItem }));

            await snsClient.send(new PublishCommand({
                TopicArn: SNS_TOPIC_ARN,
                Message: JSON.stringify({
                    schemaVersion: "1.0",
                    event_type: "HelpRequestStatusUpdated",
                    request_id: requestId,
                    incident_id: body.incident_id,
                    current_status: newItem.current_status,
                    timestamp: now,
                    traceId: traceId 
                })
            }));

            return { statusCode: 201, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ request_id: requestId, current_status: newItem.current_status, incident_verified: isIncidentVerified }) };
        }

        // ---------------------------------------------------------
        // 2. GET /v1/help-requests/{request_id}/timeline (ดูประวัติ)
        // ---------------------------------------------------------
        if (routeKey === "GET /v1/help-requests/{request_id}/timeline") {
            const requestId = event.pathParameters.request_id; 
            const response = await docClient.send(new GetCommand({ TableName: TABLE_NAME, Key: { request_id: requestId } }));
            if (!response.Item) return { statusCode: 404, body: JSON.stringify({ message: "Help request not found" }) };
            return { statusCode: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ request_id: response.Item.request_id, current_status: response.Item.current_status, timeline_logs: response.Item.timeline_logs }) };
        }

        // ---------------------------------------------------------
        // 3. PATCH /v1/help-requests/{request_id}/status (อัปเดตสถานะแบบ Manual)
        // ---------------------------------------------------------
        if (routeKey === "PATCH /v1/help-requests/{request_id}/status") {
            const requestId = event.pathParameters.request_id;
            const body = JSON.parse(event.body);
            const now = new Date().toISOString();

            const getResponse = await docClient.send(new GetCommand({ TableName: TABLE_NAME, Key: { request_id: requestId } }));
            if (!getResponse.Item) return { statusCode: 404, body: JSON.stringify({ message: "Not found" }) };

            const item = getResponse.Item;
            const newStatus = body.status || body.new_status;

            item.timeline_logs.push({
                update_id: randomUUID(),
                update_number: item.timeline_logs.length + 1,
                status_at_time: newStatus,
                note: body.note || "อัปเดตสถานะ",
                updated_at: now,
                updated_by: "Dispatcher-01"
            });
            item.current_status = newStatus;

            await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
            return { statusCode: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ request_id: item.request_id, current_status: item.current_status }) };
        }

        // ---------------------------------------------------------
        // 4. GET /v1/help-requests (ค้นหาด้วย incident_id)
        // ---------------------------------------------------------
        if (routeKey === "GET /v1/help-requests") {
            const incidentId = event.queryStringParameters?.incident_id;
            if (!incidentId) return { statusCode: 400, body: JSON.stringify({ error: "Missing incident_id" }) };

            const response = await docClient.send(new ScanCommand({
                TableName: TABLE_NAME,
                FilterExpression: "incident_id = :incId",
                ExpressionAttributeValues: { ":incId": incidentId }
            }));
            const formattedData = response.Items.map(item => ({ request_id: item.request_id, current_status: item.current_status }));
            return { statusCode: 200, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ incident_id: incidentId, data: formattedData }) };
        }

        // ---------------------------------------------------------
        // 5. NEW! POST /v1/healthcare
        // ---------------------------------------------------------
        if (routeKey === "POST /v1/healthcare") {
            const body = JSON.parse(event.body);
            const now = new Date().toISOString();
            
            const incident_id = body.incident_id || body.incidentId;
            const status = body.status || "NEW"; 
            
            const request_id_from_friend = body.request_id || body.requestId; 
            
            const lat = body.lat;
            const lon = body.lon;
            const request_type = body.request_type || body.requestType || "medical";
            const description = body.description || body.injuryDescription || "";

            if (!incident_id) {
                return { statusCode: 400, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ message: "Missing incident_id" }) };
            }

            let item;

            if (status === "NEW" || status === "new") {
                // รอบที่ 1: ตรวจสอบ Incident
                let isIncidentVerified = true;
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), 3000);
                try {
                    const verifyRes = await fetch(`${FRIEND_INCIDENT_API_URL}/${incident_id}`, { method: 'GET', signal: controller.signal });
                    clearTimeout(timeoutId);
                    isIncidentVerified = (verifyRes.status === 200);
                } catch (error) {
                    clearTimeout(timeoutId);
                    isIncidentVerified = false;
                }

                const initialStatus = isIncidentVerified ? "NEW" : "PENDING_VERIFICATION";
                const initialNote = isIncidentVerified ? "Healthcare Service แจ้งเคสผู้บาดเจ็บ" : "Healthcare แจ้งเคส (รอตรวจสอบ Incident)";

                item = {
                    request_id: `REQ-${randomUUID().substring(0, 8).toUpperCase()}`,
                    incident_id: incident_id,
                    incident_verified: isIncidentVerified,
                    location: { lat, lon },
                    request_type: request_type,
                    description: description,
                    current_status: initialStatus,
                    created_at: now,
                    timeline_logs: [{
                        update_id: randomUUID(),
                        update_number: 1,
                        status_at_time: initialStatus,
                        note: initialNote,
                        updated_at: now,
                        updated_by: "Healthcare Service"
                    }]
                };

            } else {
               
                if (request_id_from_friend) {
                    const getRes = await docClient.send(new GetCommand({
                        TableName: TABLE_NAME,
                        Key: { request_id: request_id_from_friend }
                    }));
                    item = getRes.Item;
                } else {
                  
                    const scanResponse = await docClient.send(new ScanCommand({
                        TableName: TABLE_NAME,
                        FilterExpression: "incident_id = :incId",
                        ExpressionAttributeValues: { ":incId": incident_id }
                    }));
                    if (scanResponse.Items && scanResponse.Items.length > 0) {
                        item = scanResponse.Items[0];
                    }
                }

                if (!item) return { statusCode: 404, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ message: "Request not found" }) };

                // เติม Log รอบ 2 หรือ 3
                if (status === "sent_request_hospital" && item.current_status !== "sent_request_hospital") {
                    item.timeline_logs.push({
                        update_id: randomUUID(),
                        update_number: item.timeline_logs.length + 1,
                        status_at_time: "sent_request_hospital",
                        note: "HealthCareService ได้ทำการส่ง TransferRequest ไปยังโรงพยาบาลแล้ว",
                        updated_at: now,
                        updated_by: "Healthcare Service"
                    });
                    item.current_status = "sent_request_hospital";
                } else if (status === "hospital_confirm" && item.current_status !== "hospital_confirm") {
                    item.timeline_logs.push({
                        update_id: randomUUID(),
                        update_number: item.timeline_logs.length + 1,
                        status_at_time: "hospital_confirm",
                        note: "โรงพยาบาลได้ตอบรับคำขอในการเข้ารับการรักษาแล้ว",
                        updated_at: now,
                        updated_by: "Healthcare Service"
                    });
                    item.current_status = "hospital_confirm";
                }
            }

            await docClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }));
            
            return { 
                statusCode: 200, 
                headers: { "Content-Type": "application/json" }, 
                body: JSON.stringify({ 
                    message: "Success", 
                    request_id: item.request_id, 
                    current_status: item.current_status 
                }) 
            };
        }

        return { statusCode: 404, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ message: "Route not found" }) };
    } catch (error) {
        console.error("Error processing request:", error);
        return { statusCode: 500, headers: { "Content-Type": "application/json" }, body: JSON.stringify({ message: "Internal Server Error", error: error.message }) };
    }
};