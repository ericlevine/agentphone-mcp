/**
 * Wakeshift events: wake the agent that asked when something happens, instead
 * of having it poll.
 *
 * message.received — a message arrived on one of the account's numbers.
 * Subscribed by watch_messages, which keeps the conversation subscribed (one
 * wake per message) until the subscription expires or is ended. AgentPhone has
 * no webhook this server can receive, so a poller reads the number's messages
 * with the subscriber's own credential and emits each new inbound one. It stops
 * once no conversation is subscribed to the number.
 *
 * Without WAKESHIFT_KEY the client is disabled: nothing is sent, no poller
 * runs, and every tool behaves as it did before.
 */

import { Wakeshift, encodeArgValue, type Subscribed } from "@wakeshift/sdk";
import { z } from "zod";
import { ApiError, type AgentPhoneAPI } from "./api.js";

export const wake = new Wakeshift();

/** What watch_messages hands to onSubscribe. Kept in memory, never sent to Wakeshift. */
type MessageWatchContext = { numberId: string; api: AgentPhoneAPI };

type NumberArgs = { number: string };

/** A watch ends after this long with no message delivered and no renewal (calling watch_messages again). */
export const WATCH_TTL = "24h";
// How often a poller reads the number's messages.
const POLL_EVERY_MS = 15_000;
// How many messages each poll reads (newest first). More new messages than
// this between two polls would leave the oldest of them unreported.
const PAGE_SIZE = 50;
// Consecutive failed polls before a poller gives up.
const MAX_ERRORS = 5;

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
      console.error("message.received: subscription without a credential; not polling (get_messages still works).");
      return;
    }
    startPoller(s.context);
  },
});

/** The event's routing value for a number ID (IDs may be case-sensitive). */
export function numberArg(numberId: string): NumberArgs {
  return { number: encodeArgValue(numberId) };
}

/**
 * Subscribe the calling conversation to every message on a number, and make
 * sure a poller is running for it. `api` must be bound to the caller's
 * credential. Calling it again renews the subscription.
 */
export async function watchNumber(numberId: string, api: AgentPhoneAPI) {
  const sub = await wake.currentCall().subscribe(messageReceived, numberArg(numberId), {
    ttl: WATCH_TTL,
    context: { numberId, api } satisfies MessageWatchContext,
  });
  // onSubscribe runs only for a new subscription, so a renewal restarts a
  // poller that has stopped (after errors, say) here.
  if (sub.state === "created" || sub.state === "renewed") startPoller({ numberId, api });
  return sub;
}

// One poller per number. A poller keeps the credential it started with.
const pollers = new Map<string, { stop(): void }>();

function startPoller({ numberId, api }: MessageWatchContext): void {
  if (!wake.enabled || pollers.has(numberId)) return;

  const args = numberArg(numberId);
  let stopped = false;
  let timer: NodeJS.Timeout | undefined;
  let errors = 0;
  // Message IDs, not timestamps, so clock skew between this server and
  // AgentPhone can't drop or repeat a message. The first poll records what is
  // already there.
  let seen: Set<string> | undefined;
  let ownNumber: string | undefined;

  const stop = () => {
    stopped = true;
    if (timer) clearTimeout(timer);
    pollers.delete(numberId);
  };
  pollers.set(numberId, { stop });

  const poll = async () => {
    if (stopped) return;
    try {
      if (seen) {
        // Nobody left to wake (every subscription expired or was ended): stop.
        const status = await messageReceived.status(args);
        if (status.subscriptions === 0) return stop();
      }
      ownNumber ??= (await api.getNumber(numberId)).phoneNumber;
      const page = await api.getMessages(numberId, PAGE_SIZE);
      if (seen) {
        // Newest first: emit the new inbound ones oldest first.
        const fresh = page.data.filter((m) => !seen!.has(m.id) && m.from !== ownNumber).reverse();
        for (const m of fresh) {
          await messageReceived.emit(
            args,
            { number_id: numberId, message_id: m.id, from: m.from ?? null, received_at: m.receivedAt },
            // One occurrence per message, however many times a retry sends it.
            { id: `msg-${numberId}-${m.id}`.slice(0, 200) }
          );
        }
      }
      seen = new Set(page.data.map((m) => m.id));
      errors = 0;
    } catch (err) {
      // The credential was revoked or the number is gone: retrying won't help.
      if (err instanceof ApiError && [401, 403, 404].includes(err.status)) {
        console.error(`message.received: stopped polling a number (AgentPhone answered ${err.status}).`);
        return stop();
      }
      if (++errors >= MAX_ERRORS) {
        console.error(`message.received: stopped polling a number after ${MAX_ERRORS} failed polls.`);
        return stop();
      }
    }
    if (!stopped) {
      timer = setTimeout(poll, POLL_EVERY_MS * 2 ** errors);
      timer.unref(); // never keeps the process alive
    }
  };
  void poll();
}

/** Tools that subscribe, for attach's `wrap`. */
export const WAKESHIFT_WRAP = {
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
    .describe("The number now being watched for inbound messages."),
};
