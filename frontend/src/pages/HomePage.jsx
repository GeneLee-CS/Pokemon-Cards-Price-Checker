import SiteHeader from "../components/SiteHeader";
import SearchBar from "../components/SearchBar";

export default function HomePage() {
  return (
    <div
      className="container"
      style={{
        minHeight: "100vh",
        display: "flex",
        flexDirection: "column",
        padding: "24px",
        boxSizing: "border-box",
      }}
    >
      <SiteHeader />

      <main
        style={{
          flex: 1,
          display: "flex",
          justifyContent: "center",
          paddingTop: "20vh",   // 👈 roughly 30% visual placement
        }}
      >
        <div
          style={{
            width: "100%",
            maxWidth: "820px",
            textAlign: "center",
          }}
        >
          <SearchBar />
        </div>
      </main>
    </div>
  );
}