import { timingSafeEqual } from "node:crypto";

export async function handler(event) {
  const expected = Buffer.from(event.request.privateChallengeParameters.answer ?? "");
  const answer = Buffer.from(event.request.challengeAnswer ?? "");
  event.response = {
    answerCorrect: expected.length === answer.length && timingSafeEqual(expected, answer),
  };
  return event;
}
