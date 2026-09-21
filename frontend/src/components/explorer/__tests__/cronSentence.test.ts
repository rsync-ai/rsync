import { describe, expect, it } from "vitest"
import { describeCron } from "@/components/explorer/cronSentence"

// Every sentence here must be exactly what the gateway's parser (robfig/cron v1.2.0,
// five fields) schedules. Where no short sentence is exact, the answer is null and the
// page shows the cron as typed.

describe("describeCron", () => {
  it.each([
    // What the model dialog's simple form writes (buildCron): hourly, daily, weekly.
    ["0 * * * *", "Every hour on the hour"],
    ["5 * * * *", "Every hour at 5 minutes past"],
    ["1 * * * *", "Every hour at 1 minute past"],
    ["0 3 * * *", "Every day at 03:00"],
    ["30 14 * * *", "Every day at 14:30"],
    ["0 9 * * 1", "At 09:00, on Mondays"],
    ["0 9 * * 0", "At 09:00, on Sundays"],
  ])("says the dialog's own %s as %s", (cron, sentence) => {
    expect(describeCron(cron)).toBe(sentence)
  })

  it.each([
    ["* * * * *", "Every minute"],
    ["*/15 * * * *", "Every 15 minutes"],
    ["0,30 * * * *", "Every 30 minutes"],
    ["5/15 * * * *", "Every 15 minutes from 00:05"],
    ["5 */4 * * *", "Every 4 hours from 00:05"],
    ["0 2/6 * * *", "Every 6 hours from 02:00"],
    ["0 0,6,12,18 * * *", "Every 6 hours from 00:00"],
    ["0 9-17 * * *", "Every hour from 09:00 to 17:00"],
    ["30 9-17/2 * * *", "Every 2 hours from 09:30 to 17:30"],
    // */5 does not wrap evenly (20:00, then 00:00 four hours later), so it gets an end time.
    ["0 */5 * * *", "Every 5 hours from 00:00 to 20:00"],
    ["*/15 9-17 * * *", "Every 15 minutes from 09:00 to 17:45"],
    ["* 9 * * *", "Every minute from 09:00 to 09:59"],
    ["0 9,17 * * *", "Every day at 09:00 and 17:00"],
    ["0,30 9 * * *", "Every day at 09:00 and 09:30"],
    ["0 1,5,22 * * *", "Every day at 01:00, 05:00 and 22:00"],
  ])("says the time of %s exactly", (cron, sentence) => {
    expect(describeCron(cron)).toBe(sentence)
  })

  it.each([
    ["0 9 * * 1-5", "At 09:00, Monday to Friday"],
    ["0 9 * * MON-fri", "At 09:00, Monday to Friday"],
    ["0 9 * * 1,3,5", "At 09:00, on Mondays, Wednesdays and Fridays"],
    ["0 9 * * sat,sun", "At 09:00, on Sundays and Saturdays"],
    ["*/15 * * * 1-5", "Every 15 minutes, Monday to Friday"],
    ["0 9 1 * *", "At 09:00, on day 1 of the month"],
    ["0 9 1,15 * *", "At 09:00, on days 1 and 15 of the month"],
    ["0 9 1-7 * *", "At 09:00, on days 1 to 7 of the month"],
    ["0 9 1 1 *", "At 09:00, on day 1 of the month, in January"],
    ["0 9 * jan,jul *", "Every day at 09:00, in January and July"],
    ["0 9 * 1-3 *", "Every day at 09:00, from January to March"],
    ["0 * * 12 *", "Every hour on the hour, in December"],
    ["0 9 ? * 1", "At 09:00, on Mondays"],
  ])("says the days of %s exactly", (cron, sentence) => {
    expect(describeCron(cron)).toBe(sentence)
  })

  it("says nothing about two day fields unless the gateway and Temporal agree on them", () => {
    // Neither field starred: robfig fires on a day matching EITHER, Temporal on a day
    // matching BOTH. robfig says every day here and Temporal says Mondays.
    expect(describeCron("0 9 1-31 * 1")).toBeNull()
    expect(describeCron("0 9 1 * 1")).toBeNull()
    // Both fields covering every day is every day to both.
    expect(describeCron("0 9 1-31 * 0-6")).toBe("Every day at 09:00")
    // A step on "*" stars the field, so robfig needs both too. Day-and-weekday is not sayable.
    expect(describeCron("0 9 */2 * 1")).toBeNull()
    // Both-of with the starred field covering every day leaves the other one.
    expect(describeCron("0 9 */1 * 1")).toBe("At 09:00, on Mondays")
    expect(describeCron("0 9 */2 * *")).toBeNull()
    expect(describeCron("0 9 * * */2")).toBe("At 09:00, on Sundays, Tuesdays, Thursdays and Saturdays")
  })

  it.each([
    // An interval that does not wrap evenly: */7 is :00, :07 … :56, then :00 four minutes later.
    ["*/7 * * * *"],
    ["0 1,2,5,9,22,23,12 * * *"],
    // Minutes repeating inside hours that are not one block.
    ["*/15 9,17 * * *"],
    ["0 9 1,5,9,13,20 * *"],
  ])("gives up on %s rather than say it roughly", (cron) => {
    expect(describeCron(cron)).toBeNull()
  })

  it.each([
    [""],
    [undefined],
    [null],
    ["0 3 * *"],
    ["0 0 3 * * *"],
    ["@daily"],
    ["60 * * * *"],
    ["0 24 * * *"],
    ["0 9 0 * *"],
    ["0 9 * 13 *"],
    // robfig's weekdays stop at 6; a 7 for Sunday is refused by the gateway.
    ["0 9 * * 7"],
    ["0 9 * * monday"],
    ["*/0 * * * *"],
    ["5-1 * * * *"],
    ["1-2-3 * * * *"],
    ["1/2/3 * * * *"],
    ["+5 * * * *"],
    ["L * * * *"],
  ])("returns null for %s, which the gateway would not accept or this cannot parse", (cron) => {
    expect(describeCron(cron)).toBeNull()
  })

  it("ignores surrounding and repeated whitespace, as the parser does", () => {
    expect(describeCron("  0   3 *\t* *  ")).toBe("Every day at 03:00")
  })
})
