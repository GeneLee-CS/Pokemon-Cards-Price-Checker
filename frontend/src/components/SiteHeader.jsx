import { Link } from "react-router-dom";

export default function SiteHeader() {
  return (
    <header
      style={{
        width: "100%",
        padding: "20px 0",
        borderBottom: "1px solid #eee",
        marginBottom: "24px",
        display: "flex",
        justifyContent: "center", // 👈 centers everything
      }}
    >
      <Link
        to="/"
        style={{
          textDecoration: "none",
          color: "inherit",
        }}
      >
        <h1
          style={{
            margin: 0,
            fontSize: "26px",
            fontWeight: "600",
            textAlign: "center",
          }}
        >
          Poke Price Checker
        </h1>
      </Link>
    </header>
  );
}