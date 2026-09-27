/**
 * Copying text to the clipboard, with a toast that tells the truth.
 *
 * Both call sites used to do this:
 *
 *     navigator.clipboard.writeText(text)
 *     toast.success("Copied to clipboard")
 *
 * `writeText` returns a promise. It rejects on an insecure origin, on a denied
 * permission, and when the document is not focused -- and `navigator.clipboard`
 * itself is undefined on a plain-HTTP origin, which throws synchronously. In
 * every one of those cases the user saw "Copied", pasted whatever was already
 * on their clipboard, and had nothing to tell them otherwise. On the admin
 * invitations page the payload is a single-use invite link, so the failure mode
 * is mailing someone the wrong thing.
 */
export async function copyTextToClipboard(text: string): Promise<boolean> {
  try {
    await navigator.clipboard.writeText(text)
    return true
  } catch {
    return false
  }
}
