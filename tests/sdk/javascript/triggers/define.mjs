// An AWS-shaped fixture. The emulator owns orchestration; this handler selects
// the next challenge using the authenticated results in the supplied session.
export async function handler(event) {
  const session = event.request.session ?? [];
  const last = session.at(-1);
  event.response = { issueTokens: false, failAuthentication: false };
  if (event.request.userNotFound || !last?.challengeResult) {
    event.response.failAuthentication = true;
  } else if (last.challengeName === "SRP_A") {
    event.response.challengeName = "PASSWORD_VERIFIER";
  } else if (last.challengeName === "PASSWORD_VERIFIER") {
    event.response.challengeName = "CUSTOM_CHALLENGE";
  } else if (last.challengeName === "CUSTOM_CHALLENGE") {
    event.response.issueTokens = true;
  } else {
    throw new Error(`Unexpected challenge ${last.challengeName}`);
  }
  return event;
}
