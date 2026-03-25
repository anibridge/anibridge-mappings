const MAPPING_QUERY_PARAM = "mapping";

export const getSelectedIdFromUrl = (): number | null => {
  if (typeof window === "undefined") return null;
  const value = new URLSearchParams(window.location.search).get(MAPPING_QUERY_PARAM);
  if (!value) return null;
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : null;
};

export const setSelectedIdInUrl = (selectedId: number | null, replace = false) => {
  if (typeof window === "undefined") return;
  const url = new URL(window.location.href);
  if (selectedId === null) {
    url.searchParams.delete(MAPPING_QUERY_PARAM);
  } else {
    url.searchParams.set(MAPPING_QUERY_PARAM, String(selectedId));
  }

  const nextUrl = `${url.pathname}${url.search}${url.hash}`;
  if (replace) {
    window.history.replaceState(null, "", nextUrl);
    return;
  }

  window.history.pushState(null, "", nextUrl);
};
