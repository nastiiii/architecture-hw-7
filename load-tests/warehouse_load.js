import http from "k6/http";
import { check, sleep } from "k6";

export const options = {
  vus: 10,
  duration: "30s",
  thresholds: {
    http_req_failed: ["rate<0.01"],
    http_req_duration: ["p(95)<500"],
  },
};

const producerUrl = __ENV.PRODUCER_URL || "http://localhost:8000";

function uuidv4() {
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (char) => {
    const value = Math.floor(Math.random() * 16);
    const nibble = char === "x" ? value : (value & 0x3) | 0x8;
    return nibble.toString(16);
  });
}

export default function () {
  const eventId = uuidv4();
  const payload = JSON.stringify({
    event_id: eventId,
    event_type: "PRODUCT_RECEIVED",
    product_id: `SKU-LOAD-${__VU}-${__ITER}`,
    zone_id: "ZONE-LOAD",
    quantity: 1,
    timestamp: new Date().toISOString(),
  });

  const response = http.post(`${producerUrl}/events`, payload, {
    headers: {"Content-Type": "application/json"},
  });

  check(response, {
    "event accepted": (res) => res.status === 200,
    "response has event_id": (res) => res.json("event_id") === eventId,
  });
  sleep(1);
}
