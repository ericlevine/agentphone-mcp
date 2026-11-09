/**
 * Wakeshift events: wake the agent that asked when something happens, instead
 * of having it poll.
 *
 * message.received — a message arrived on one of the account's numbers.
 * Subscribed by send_message (to hear the reply) and watch_messages. AgentPhone
 * has no webhook this server can receive, so a watch polls the number's
 * messages with the subscriber's own credential and emits on the first new
 * inbound one.
 *
 * Without WAKESHIFT_KEY the client is disabled: nothing is sent, no watch runs,
 * and every tool behaves as it did before.
 */

import { Wakeshift, encodeArgValue, type Subscribed } from "@wakeshift/sdk";
import { z } from "zod";
import { ApiError, type AgentPhoneAPI } from "./api.js";

export const wake = new Wakeshift();

/** What a subscribing tool hands to onSubscribe. Kept in memory, never sent to Wakeshift. */
type MessageWatchContext = { numberId: string; api: AgentPhoneAPI };

type NumberArgs = { number: string };

// How often a watch reads the number's messages, and how long it waits for one.
const WATCH_EVERY = "15s";
const WATCH_TIMEOUT = "24h";

export const messageReceived = wake.defineEvent("message.received", {
  description: "A message arrived on one of your AgentPhone numbers",
  args: z.object({ number: z.string() }),
  payload: z.object({
    number_id: z.string(),
    message_id: z.string(),
    from: z.string().nullable(),
    received_at: z.string(),
  }),
  onSubscribe: (s: Subscribed<NumberArgs, MessageWatchContext>) => {
    if (!s.context) {
      // A subscription this process didn't make (another instance redeemed the
      // voucher): there is no credential here to read the number with.
      console.error("message.received: subscription without a credential; not watching (get_messages still works).");
      return;
    }
    watchNumber(s.context);
  },
});

/** The event's routing value for a number ID (IDs may be case-sensitive). */
export function numberArg(numberId: string): NumberArgs {
  return { number: encodeArgValue(numberId) };
}

/**
 * Subscribe the calling conversation to the next message on a number.
 * `api` must already be bound to the caller's credential.
 */
export async function subscribeToNextMessage(numberId: string, api: AgentPhoneAPI) {
  return wake.currentCall().subscribe(messageReceived, numberArg(numberId), {
    once: true,
    ttl: WATCH_TIMEOUT,
    context: { numberId, api } satisfies MessageWatchContext,
  });
}

function watchNumber({ numberId, api }: MessageWatchContext): void {
  // The first check records what is already there; later checks look for an
  // inbound message that wasn't. Message IDs, not timestamps, so clock skew
  // between this server and AgentPhone can't drop or repeat one.
  let ownNumber: string | undefined;
  let seen: Set<string> | undefined;

  messageReceived.watch(
    numberArg(numberId),
    async () => {
      if (!seen) {
        ownNumber = (await api.getNumber(numberId)).phoneNumber;
        seen = new Set((await api.getMessages(numberId, 50)).data.map((m) => m.id));
        return undefined;
      }
      const page = await api.getMessages(numberId, 50);
      // Newest first: the last new inbound message is the earliest to arrive.
      const fresh = page.data.filter((m) => !seen!.has(m.id) && m.from !== ownNumber);
      const first = fresh[fresh.length - 1];
      if (!first) return undefined;
      return {
        number_id: numberId,
        message_id: first.id,
        from: first.from ?? null,
        received_at: first.receivedAt,
      };
    },
    {
      every: WATCH_EVERY,
      timeout: WATCH_TIMEOUT,
      // The credential was revoked or the number is gone: retrying won't help.
      isFinal: (err) => err instanceof ApiError && [401, 403, 404].includes(err.status),
    }
  );
}

/** Tools that subscribe, for attach's `wrap`. */
export const WAKESHIFT_WRAP = {
  send_message: [messageReceived],
  watch_messages: [messageReceived],
};

/**
 * Output schemas of the tools registered only when Wakeshift is on. Kept apart
 * from OUTPUT_SCHEMAS, which must match the tools registered without a key.
 */
export const WAKESHIFT_OUTPUT_SCHEMAS: Record<string, z.ZodTypeAny> = {
  watch_messages: z
    .object({
      number_id: z.string().describe("The number being watched."),
      phone_number: z.string().describe("Its phone number, in E.164 form."),
    })
    .describe("The number now being watched for its next inbound message."),
};
