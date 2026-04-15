import { useEffect, useState } from "react";
import { useParams } from "react-router";
import SiteHeader from "../components/SiteHeader";
import { getCardDetail, getCardListings } from "../services/api";

export default function CardPage() {
  const { cardId } = useParams();

  const [cardDetail, setCardDetail] = useState(null);
  const [listings, setListings] = useState([]);
  const [cardLoading, setCardLoading] = useState(true);
  const [listingsLoading, setListingsLoading] = useState(false);
  const [listingsLoadedOnce, setListingsLoadedOnce] = useState(false);
  const [cardError, setCardError] = useState("");
  const [listingsError, setListingsError] = useState("");
  const [sortOrder, setSortOrder] = useState("price_asc");
  const [listingsLimit, setListingsLimit] = useState(20);

  useEffect(() => {
    let cancelled = false;

    async function loadCardDetail() {
      setCardLoading(true);
      setCardError("");
      setCardDetail(null);

      setListings([]);
      setListingsError("");
      setListingsLoading(false);
      setListingsLoadedOnce(false);
      setSortOrder("price_asc");
      setListingsLimit(20);

      try {
        const detailData = await getCardDetail(cardId);

        if (cancelled) return;
        setCardDetail(detailData);
      } catch (err) {
        if (cancelled) return;
        console.error("DETAIL ERROR:", err);
        setCardError("Failed to load card details");
      } finally {
        if (!cancelled) {
          setCardLoading(false);
        }
      }
    }

    loadCardDetail();

    return () => {
      cancelled = true;
    };
  }, [cardId]);

  useEffect(() => {
    if (!cardDetail || cardLoading || cardError) {
      return;
    }

    let cancelled = false;

    async function loadListings() {
      setListingsLoading(true);
      setListingsError("");

      try {
        const listingsData = await getCardListings(cardId, sortOrder, listingsLimit);

        if (cancelled) return;

        if (Array.isArray(listingsData.listings)) {
          setListings(listingsData.listings);
        } else {
          setListingsError("Listings response shape was unexpected");
        }
      } catch (err) {
        if (cancelled) return;
        console.error("LISTINGS ERROR:", err);
        setListingsError("Failed to load listings");
      } finally {
        if (!cancelled) {
          setListingsLoading(false);
          setListingsLoadedOnce(true);
        }
      }
    }

    loadListings();

    return () => {
      cancelled = true;
    };
  }, [cardId, cardDetail, cardLoading, cardError, sortOrder, listingsLimit]);

  if (cardLoading) {
    return <div className="container">Loading card...</div>;
  }

  if (cardError || !cardDetail || !cardDetail.card) {
    return (
      <div className="container">
        <SiteHeader />
        <div className="card">
          <h1>Card page</h1>
          <p>{cardError || "Card not found"}</p>
        </div>
      </div>
    );
  }

  const card = cardDetail.card;
  const latestPrice = cardDetail.latest_tcg_price;
  const ebayMarket = cardDetail.ebay_market;

  const displayedListingCount =
    ebayMarket?.listing_count === 200 ? "200+" : ebayMarket?.listing_count;

  const canLoadMore = listingsLimit < 200 && listings.length >= listingsLimit;

  return (
    <div className="container">
      <SiteHeader />

      <div
        className="card"
        style={{
          display: "flex",
          gap: "28px",
          alignItems: "flex-start",
          background: "#ffffff",
          borderRadius: "16px",
          padding: "24px",
          boxShadow: "0 2px 8px rgba(0,0,0,0.05)"
        }}
      >
        <div
          style={{
            flex: "0 0 400px",
            display: "flex",
            justifyContent: "center",
          }}
        >
          {card.image_large_url ? (
            <img
              src={card.image_large_url}
              alt={card.card_name}
              style={{
                width: "400px",
                height: "auto",
                borderRadius: "12px",
                display: "block",
              }}
            />
          ) : (
            <div
              style={{
                width: "400px",
                minHeight: "265px",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                border: "1px solid #ddd",
                borderRadius: "12px",
              }}
            >
              No image available
            </div>
          )}
        </div>

        <div
          style={{
            flex: 1,
            display: "flex",
            flexDirection: "column",
            justifyContent: "space-between",
            minHeight: "265px",
          }}
        >
          <div>
            <h1 style={{
              marginTop: 0,
              marginBottom: "16px",
              fontSize: "36px",
              fontWeight: 700,
              color: "#111"
            }}>
              {card.card_name}
            </h1>

            <p style={{ margin: "8px 0", color: "#222" }}>
              <span style={{ color: "#666" }}>Set:</span> {card.set_name}
            </p>

            <p style={{ margin: "8px 0", color: "#222" }}>
              <span style={{ color: "#666" }}>Number:</span> {card.card_number}
            </p>

            {card.rarity && (
              <p style={{ margin: "8px 0", color: "#222" }}>
                <span style={{ color: "#666" }}>Rarity:</span> {card.rarity}
              </p>
            )}

            {card.release_date && (
              <p style={{ margin: "8px 0", color: "#222" }}>
                <span style={{ color: "#666" }}>Release Date:</span> {card.release_date}
              </p>
            )}
          </div>

          <div style={{ marginTop: "23px" }}>
            <h2 style={{ marginBottom: "12px", fontSize: "20px" }}>
              TCGplayer Market Price
            </h2>

            {latestPrice ? (
              <>
                <p
                  style={{
                    fontSize: "32px",
                    fontWeight: 700,
                    color: "#16a34a",
                    margin: "0 0 8px 0",
                  }}
                >
                  ${Number(latestPrice.market_price).toFixed(2)}
                </p>

                <p style={{ margin: 0, color: "#666" }}>
                  <span style={{ color: "#666" }}>Price Date:</span> {latestPrice.price_date}
                </p>
              </>
            ) : (
              <p>No price available</p>
            )}
          </div>

          <div style={{ marginTop: "23px" }}>
            <h3 style={{ marginBottom: "10px", fontSize: "20px" }}>eBay Market</h3>

            {ebayMarket ? (
              <>
                <p style={{ margin: "8px 0", color: "#222" }}>
                  <span style={{ color: "#666" }}>Total Listings:</span>{" "}
                  {displayedListingCount}
                </p>

                <p style={{ margin: "8px 0", color: "#222" }}>
                  <span style={{ color: "#666" }}>PSA Graded Listings:</span>{" "}
                  {Number(ebayMarket.graded_listing_count)}
                </p>

                <p style={{ margin: "8px 0", color: "#222" }}>
                  <span style={{ color: "#666" }}>Median:</span>{" "}
                  ${Number(ebayMarket.median_price).toFixed(2)}
                </p>

                <p style={{ margin: "8px 0", color: "#222" }}>
                  <span style={{ color: "#666" }}>Range:</span>{" "}
                  ${Number(ebayMarket.min_price).toFixed(2)} – ${Number(ebayMarket.max_price).toFixed(2)}
                </p>
              </>
            ) : (
              <p>eBay market summary not available</p>
            )}
          </div>
        </div>
      </div>

      <div className="card">
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
            marginBottom: "16px",
            gap: "16px",
            flexWrap: "wrap",
          }}
        >
          <h2 style={{ margin: 0 }}>Listings</h2>

          <div style={{ display: "flex", alignItems: "center", gap: "10px" }}>
            <label htmlFor="listing-sort" style={{ color: "#666" }}>
              Sort by
            </label>
            <select
              id="listing-sort"
              value={sortOrder}
              onChange={(e) => {
                setSortOrder(e.target.value);
                setListingsLimit(20);
              }}
              style={{
                padding: "8px 12px",
                borderRadius: "8px",
                border: "1px solid #d1d5db",
                backgroundColor: "#fff",
                cursor: "pointer",
              }}
            >
              <option value="price_asc">Price: Low to High</option>
              <option value="price_desc">Price: High to Low</option>
            </select>
          </div>
        </div>

        {listingsError ? (
          <p>{listingsError}</p>
        ) : !listingsLoadedOnce && listingsLoading ? (
          <p>Loading listings...</p>
        ) : listings.length === 0 ? (
          <p>Listing data not available</p>
        ) : (
          <>
            <div>
              {listings.map((listing, index) => (
                <div
                  key={`${listing.listing_id}-${listing.listing_url}`}
                  style={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    gap: "16px",
                    padding: "14px 0",
                    borderBottom: "1px solid #e5e7eb",
                  }}
                >
                  <a
                    href={listing.listing_url}
                    target="_blank"
                    rel="noreferrer"
                    title={listing.title || "Untitled listing"}
                    style={{
                      color: "#2563eb",
                      textDecoration: "none",
                      flex: 1,
                      minWidth: 0,
                      lineHeight: 1.4,
                    }}
                  >
                    {listing.title || "Untitled listing"}
                  </a>

                  <div
                    style={{
                      fontWeight: 600,
                      color: "#111",
                      whiteSpace: "nowrap",
                      flexShrink: 0,
                    }}
                  >
                    ${Number(listing.price ?? listing.price_value ?? 0).toFixed(2)}{" "}
                    {listing.currency ?? ""}
                  </div>
                </div>
              ))}
            </div>

            {canLoadMore && (
              <div style={{ marginTop: "18px" }}>
                <button
                  type="button"
                  onClick={() =>
                    setListingsLimit((prev) => Math.min(prev + 20, 200))
                  }
                  disabled={listingsLoading}
                  style={{
                    padding: "10px 16px",
                    borderRadius: "8px",
                    border: "1px solid #d1d5db",
                    backgroundColor: "#fff",
                    cursor: listingsLoading ? "default" : "pointer",
                    fontWeight: 500,
                  }}
                >
                  {listingsLoading ? "Loading..." : "Load more"}
                </button>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}