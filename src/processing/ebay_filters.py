import re


NON_CARD_KEYWORDS = {
    "proxy",
    "fan art",
    "fanart",
    "fan made",
    "fan-made",
    "custom",
    "fake",
    "unofficial",
    "replica",
    "reprint",
    "sticker",
    "poster",
    "print",
    "display",
    "art case",
    "artwork case",
    "fan made",
    "fan-made",
    "fanmade",

    # proxy-style foil language
    "gold foil",
    "gold-foil",
    "goldfoil",
    "custom foil",
    "rainbow foil",
    "orange foil",
}

CUSTOM_FOIL_PATTERN = re.compile(
    r"\b(gold|silver|orange|blue|red|green|pink|purple|rainbow)\s*foil\b",
    re.IGNORECASE,
)

def is_non_card_listing(title_normalized: str) -> bool:

    for keyword in NON_CARD_KEYWORDS:
        if keyword in title_normalized:
            return True
    
    if CUSTOM_FOIL_PATTERN.search(title_normalized):
        return True
    
    return False