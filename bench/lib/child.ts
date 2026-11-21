// Starting benchmark child processes and talking to them over Node's IPC
// channel. Each child sends "ready" once connected to the database, then waits
// for "go", so every process starts working at the same moment: otherwise the
// first process to boot would drain jobs alone for a while and skew the result.
import { fork, type ChildProcess } from "node:child_process";
import { join } from "node:path";

export type Message = { type: string; [key: string]: unknown };

export class Child {
  private readonly proc: ChildProcess;
  private readonly inbox: Message[] = [];
  private readonly waiters: { type: string; resolve: (m: Message) => void; reject: (e: Error) => void }[] = [];
  private exitError: Error | null = null;

  constructor(script: string, args: string[]) {
    this.proc = fork(join(import.meta.dirname, "..", script), args, {
      execArgv: ["--import", "tsx"],
      stdio: ["ignore", "inherit", "inherit", "ipc"],
    });
    this.proc.on("message", (m) => {
      this.inbox.push(m as Message);
      this.deliver();
    });
    this.proc.on("exit", (code, signal) => {
      if (code === 0) return;
      this.exitError = new Error(`${script} exited with ${signal ?? `code ${code}`}`);
      for (const w of this.waiters.splice(0)) w.reject(this.exitError);
    });
  }

  send(message: Message): void {
    this.proc.send(message);
  }

  /** The next message of this type (waiting for it if needed). */
  next(type: string): Promise<Message> {
    return new Promise((resolve, reject) => {
      if (this.exitError) return reject(this.exitError);
      this.waiters.push({ type, resolve, reject });
      this.deliver();
    });
  }

  kill(): void {
    if (this.proc.exitCode === null) this.proc.kill();
  }

  private deliver(): void {
    for (let i = 0; i < this.waiters.length; ) {
      const w = this.waiters[i]!;
      const at = this.inbox.findIndex((m) => m.type === w.type);
      if (at === -1) {
        i++;
        continue;
      }
      w.resolve(this.inbox.splice(at, 1)[0]!);
      this.waiters.splice(i, 1);
    }
  }
}

/** Start `count` children, wait until all are ready, then release them together. */
export async function startTogether(make: (i: number) => Child, count: number): Promise<Child[]> {
  const children = Array.from({ length: count }, (_, i) => make(i));
  try {
    await Promise.all(children.map((c) => c.next("ready")));
  } catch (err) {
    for (const c of children) c.kill();
    throw err;
  }
  for (const c of children) c.send({ type: "go" });
  return children;
}

/** In a child: a promise for the next message of this type from the parent. */
export function fromParent(type: string): Promise<Message> {
  return new Promise((resolve) => {
    const onMessage = (m: unknown) => {
      if ((m as Message).type !== type) return;
      process.off("message", onMessage);
      resolve(m as Message);
    };
    process.on("message", onMessage);
  });
}

/** In a child: send a message; resolves once it has been handed to the IPC channel. */
export function toParent(message: Message): Promise<void> {
  return new Promise((resolve, reject) => process.send!(message, (err: Error | null) => (err ? reject(err) : resolve())));
}

/** In a child: the last message. Closing the IPC channel lets the process exit on its own. */
export async function finish(message: Message): Promise<void> {
  await toParent(message);
  process.disconnect!();
}
