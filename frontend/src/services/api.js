const API_BASE_URL = "http://3.134.81.19";

export async function searchCards(query) {
  const response = await fetch(
    `${API_BASE_URL}/search?query=${encodeURIComponent(query)}`
  );

  if (!response.ok) {
    throw new Error(`Failed to search cards: ${response.status}`);
  }

  return response.json();
}

export async function getCardDetail(cardId) {
  const url = `${API_BASE_URL}/cards/${cardId}`;
  console.log("GET CARD DETAIL URL:", url);

  const response = await fetch(url);
  console.log("GET CARD DETAIL STATUS:", response.status);

  const text = await response.text();
  console.log("GET CARD DETAIL RAW BODY:", text);

  if (!response.ok) {
    throw new Error(`Failed to fetch card details: ${response.status} ${text}`);
  }

  return JSON.parse(text);
}

export async function getCardListings(
  cardId,
  sort = "price_asc",
  limit = 20
) {
  const url = `${API_BASE_URL}/cards/${cardId}/listings?sort=${sort}&limit=${limit}`;
  console.log("GET CARD LISTINGS URL:", url);

  const response = await fetch(url);
  console.log("GET CARD LISTINGS STATUS:", response.status);

  const text = await response.text();
  console.log("GET CARD LISTINGS RAW BODY:", text);

  if (!response.ok) {
    throw new Error(`Failed to load listings: ${response.status} ${text}`);
  }

  return JSON.parse(text);
}