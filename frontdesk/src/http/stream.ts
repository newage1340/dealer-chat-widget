/**
 * The WebSocket Twilio Media Streams connect to (VOICE_PIPELINE=media_streams or media_streams_local).
 * Each connection is one call, run by a StreamCall.
 */

import type { UpgradeWebSocket } from "hono/ws";

import { TwilioSignature } from "../providers/twilio.ts";
import { StreamCall } from "../voice/stream.ts";
import type { VoicePipeline } from "../voice/pipeline.ts";
import type { ChannelDeps } from "./context.ts";

/**
 * The public wss:// address of the stream endpoint, for the <Stream> TwiML and for checking Twilio's signature.
 */
export function streamUrl(publicBaseUrl: string, host: string): string {
    const base = publicBaseUrl || `https://${host}`;
    return `${base.replace(/^http/, "ws")}/voice/stream`;
}

export function streamUpgrade(
    deps: ChannelDeps,
    pipeline: Extract<VoicePipeline, { kind: "media_streams" | "media_streams_local" }>,
    upgrade: UpgradeWebSocket
)
{
    const { config, services, engine } = deps;
    return upgrade(c => {
        // Twilio signs the upgrade request like any webhook: the wss URL, no parameters.
        const signature = c.req.header("X-Twilio-Signature") ?? "";
        const trusted = !config.validateTwilio
            || TwilioSignature.valid(config.twilioAuthToken, streamUrl(config.publicBaseUrl, c.req.header("host") ?? ""), {}, signature);
        let call: StreamCall | null = null;
        return {
            onOpen(_event, ws) {
                if (!trusted) {
                    ws.close(1008, "invalid signature");
                    return;
                }
                call = new StreamCall({ services, engine, pipeline }, {
                    send:  message => ws.send(JSON.stringify(message)),
                    close: () => ws.close(),
                });
            },
            onMessage(event) {
                void call?.handle(String(event.data)).catch(e => console.error("media stream message failed:", e));
            },
            onClose() {
                void call?.finish();
            },
        };
    });
}
