const http = require('http');
const BROKER_URL = "http://34.118.234.194:8091";
const GEMINI_KEY = "REDACTED";

/**
 * NanoClaw Substrate Agent
 * 
 * This script represents the modified NanoClaw Task Scheduler.
 * Instead of internal cron-polling, it implements the "Receiver Pattern"
 * by fetching tasks from the external broker and pushing results back.
 */
async function run() {
    try {
        const taskRes = await fetch(BROKER_URL + "/api/get-task");
        const data = await taskRes.json();
        if (!data.task) return;

        const rawTask = data.task;
        const task = rawTask.includes(':') ? rawTask.split(':').pop().trim() : rawTask;
        
        let result = "";
        try {
            const prompt = "Task: " + task + ". Output exactly in this format: [THINKING]: your logic [TOOLS]: tools used [RESPONSE]: your answer";
            const geminiRes = await fetch('https://generativelanguage.googleapis.com/v1beta/models/gemini-flash-latest:generateContent?key=' + GEMINI_KEY, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ contents: [{ parts: [{ text: prompt }] }] })
            });
            const gData = await geminiRes.json();
            
            if (gData.candidates?.[0]?.content?.parts[0]?.text) {
                result = gData.candidates[0].content.parts[0].text;
            } else {
                result = "[THINKING]: API Error.\n[TOOLS]: gemini_api\n[RESPONSE]: Error: " + (gData.error?.message || "No candidates found");
            }
        } catch (e) {
            result = "[THINKING]: Network failure.\n[TOOLS]: fetch\n[RESPONSE]: Error: " + e.message;
        }

        // Proactive Push: Inform the broker that the task is complete
        await fetch(BROKER_URL + "/api/task-result", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ actorId: data.actorId, result })
        });
    } catch (e) { console.error(e); }
}

// Health check endpoint for Substrate Rehydration
http.createServer((req, res) => { res.writeHead(200); res.end('READY'); }).listen(8080);

// Poll the broker for tasks while the actor is resumed (physical Pod is running)
setInterval(run, 3000);
run();
