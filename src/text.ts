/**
 * Text both backends can store identically. PostgreSQL TEXT can't hold U+0000,
 * and a lone UTF-16 surrogate (half of a pair, which a JS string may contain)
 * has no UTF-8 encoding: node-postgres silently turns it into U+FFFD. SQLite
 * accepts both. These helpers make the two backends agree (#18).
 */

// A high surrogate not followed by a low one, or a low one not preceded by a high one.
const LONE_SURROGATE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g;
// eslint-disable-next-line no-control-regex -- matching U+0000 is the point
const NUL = /\u0000/g;

/**
 * Free text where an approximation beats an error, such as a handler's error
 * message: U+0000 and lone surrogates become U+FFFD, the replacement character.
 * A message quoting binary input is still recorded, instead of fail() throwing.
 */
export function toStorableText(text: string): string {
  return text.replace(NUL, "�").replace(LONE_SURROGATE, "�");
}

/**
 * Identifiers (queue names, idempotency keys, worker ids) must be stored
 * exactly, or two different values could collide, so they are rejected instead.
 */
export function assertStorableIdentifier(name: string, value: string): void {
  NUL.lastIndex = 0;
  LONE_SURROGATE.lastIndex = 0;
  if (NUL.test(value) || LONE_SURROGATE.test(value)) {
    throw new RangeError(`${name} must not contain U+0000 or unpaired surrogates`);
  }
}
