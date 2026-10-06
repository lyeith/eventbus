export async function handler(event) {
  event.response = { issueTokens: "wrong-type", failAuthentication: false };
  return event;
}
