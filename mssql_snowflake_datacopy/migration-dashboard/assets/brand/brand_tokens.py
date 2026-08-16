"""
Tiger Analytics — Brand Tokens for Streamlit Apps
===================================================

Single source of truth for all brand colors, typography, and chart palettes.
Import this module in any Streamlit app to ensure consistent branding.

Usage:
    from assets.brand.brand_tokens import BRAND, get_theme_tokens

Maintained by: Tiger Analytics Design & Engineering
Last updated:  2026-04-02
"""

# ---------------------------------------------------------------------------
# Core Brand Palette
# ---------------------------------------------------------------------------

BRAND = {
    # ── Primary ──────────────────────────────────────────────────────────
    "orange":           "#F15A22",   # Primary accent
    "navy":             "#1A1A2E",   # Primary dark
    "white":            "#FFFFFF",   # Light backgrounds

    # ── Secondary ────────────────────────────────────────────────────────
    "orange_dark":      "#D94E1C",   # Hover / pressed
    "orange_light":     "#FF7A47",   # Tags, badges
    "blue":             "#2196F3",   # Info, secondary chart
    "green":            "#4CAF50",   # Success, positive delta
    "red":              "#E53935",   # Error, negative delta
    "amber":            "#FF9800",   # Warning

    # ── Neutral Scale ────────────────────────────────────────────────────
    "grey_50":          "#FAFAFA",
    "grey_100":         "#F5F5F5",
    "grey_200":         "#E0E0E0",
    "grey_400":         "#9E9E9E",
    "grey_700":         "#4A4A68",
    "grey_900":         "#1A1A2E",

    # ── Dark Mode ────────────────────────────────────────────────────────
    "dark_bg":          "#0D1117",
    "dark_surface":     "#161B22",
    "dark_border":      "#2D333B",
    "dark_text":        "#E6EDF3",
    "dark_text_muted":  "#8B949E",

    # ── Chart Color Sequences ────────────────────────────────────────────
    "chart_categorical": [
        "#F15A22", "#2196F3", "#4CAF50", "#FF9800",
        "#9C27B0", "#00BCD4", "#E91E63", "#8BC34A",
    ],
    "chart_sequential_orange": [
        "#FFF3E0", "#FFE0B2", "#FFCC80", "#FFB74D",
        "#FFA726", "#FF9800", "#F15A22", "#D94E1C",
    ],
    "chart_diverging": [
        "#1565C0", "#42A5F5", "#90CAF9", "#E0E0E0",
        "#FFCC80", "#FF9800", "#F15A22",
    ],

    # ── Typography ───────────────────────────────────────────────────────
    "font_stack": "'Source Sans Pro', 'Segoe UI', 'Helvetica Neue', Arial, sans-serif",
}


# ---------------------------------------------------------------------------
# Theme-Specific Token Sets
# ---------------------------------------------------------------------------

def get_theme_tokens(theme: str = "light") -> dict:
    """Return a flat dict of tokens resolved for the given theme.

    Args:
        theme: 'light' or 'dark'

    Returns:
        Dict with keys: bg, surface, text, text_muted, border, accent,
        accent_hover, sidebar_bg, sidebar_text, chart_grid
    """
    if theme == "dark":
        return {
            "bg":            BRAND["dark_bg"],
            "surface":       BRAND["dark_surface"],
            "text":          BRAND["dark_text"],
            "text_muted":    BRAND["dark_text_muted"],
            "border":        BRAND["dark_border"],
            "accent":        BRAND["orange"],
            "accent_hover":  BRAND["orange_dark"],
            "sidebar_bg":    "#010409",
            "sidebar_text":  BRAND["dark_text"],
            "chart_grid":    BRAND["dark_border"],
        }
    else:
        return {
            "bg":            BRAND["white"],
            "surface":       BRAND["grey_100"],
            "text":          BRAND["navy"],
            "text_muted":    BRAND["grey_700"],
            "border":        BRAND["grey_200"],
            "accent":        BRAND["orange"],
            "accent_hover":  BRAND["orange_dark"],
            "sidebar_bg":    BRAND["navy"],
            "sidebar_text":  BRAND["white"],
            "chart_grid":    BRAND["grey_200"],
        }
