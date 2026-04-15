import { useEffect, useState } from "react";
import { useNavigate } from "react-router";
import { searchCards } from "../services/api";

export default function SearchBar() {
  const [query, setQuery] = useState("");
  const [debouncedQuery, setDebouncedQuery] = useState("");
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);

  const navigate = useNavigate();

  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedQuery(query);
    }, 350);

    return () => clearTimeout(timer);
  }, [query]);

  useEffect(() => {
    async function runSearch() {
      if (debouncedQuery.trim().length < 2) {
        setResults([]);
        return;
      }

      try {
        setLoading(true);
        const data = await searchCards(debouncedQuery);
        setResults(data);
      } catch (error) {
        console.error("Search failed:", error);
        setResults([]);
      } finally {
        setLoading(false);
      }
    }

    runSearch();
  }, [debouncedQuery]);

  function handleSelect(cardId) {
    setQuery("");
    setResults([]);
    navigate(`/cards/${cardId}`);
  }

  return (
    <div style={{ position: "relative", width: "100%" }}>
      <input
        type="text"
        placeholder="Try: Charizard Base  4/102"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        style={{
          width: "100%",
          padding: "18px 20px",
          fontSize: "18px",
          border: "1px solid #ccc",
          borderRadius: "12px",
          boxSizing: "border-box",
        }}
      />

      {loading && (
        <p style={{ marginTop: "10px", fontSize: "14px" }}>Searching...</p>
      )}

      {results.length > 0 && (
        <div
          style={{
            position: "absolute",
            top: "calc(100% + 8px)",
            left: 0,
            right: 0,
            border: "1px solid #ddd",
            borderRadius: "12px",
            background: "white",
            overflow: "hidden",
            boxShadow: "0 6px 18px rgba(0, 0, 0, 0.08)",
            zIndex: 10,
          }}
        >
          {results.map((card, index) => (
            <button
              key={card.card_id}
              onClick={() => handleSelect(card.card_id)}
              style={{
                display: "block",
                width: "100%",
                textAlign: "left",
                background: "white",
                border: "none",
                borderBottom:
                  index !== results.length - 1 ? "1px solid #eee" : "none",
                padding: "14px 16px",
                cursor: "pointer",
              }}
            >
              <strong>{card.card_name}</strong>
              <br />
              <small>
                {card.set_name} - {card.card_number}
              </small>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}