import Keycloak from "keycloak-js";

const keycloak = new Keycloak({
  url: "http://localhost:8080",
  realm: "ptvn",
  clientId: "agent-frontend",
});

await keycloak.init({
  onLoad: "login-required",
  pkceMethod: "S256",
});

export async function runAgent(message: string) {
  await keycloak.updateToken(30);

  const response = await fetch("/agents/world-agent/runs", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${keycloak.token}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ message }),
  });

  if (!response.ok) {
    throw new Error(`Agent request failed: ${response.status}`);
  }

  return response.json();
}
