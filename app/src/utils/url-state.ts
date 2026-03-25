const MAPPING_QUERY_PARAM = "mapping";
const MAPPING_KEY_SEPARATOR = "->";

export const buildDescriptorMappingKey = (
  sourceDescriptor: string,
  targetDescriptor: string,
) => `${sourceDescriptor}${MAPPING_KEY_SEPARATOR}${targetDescriptor}`;

export const getSelectedMappingKeyFromUrl = (): string | null => {
  if (typeof window === "undefined") return null;
  const value = new URLSearchParams(window.location.search).get(
    MAPPING_QUERY_PARAM,
  );
  return value?.trim() ? value : null;
};

export const setSelectedMappingKeyInUrl = (
  mappingKey: string | null,
  replace = false,
) => {
  if (typeof window === "undefined") return;
  const url = new URL(window.location.href);
  if (!mappingKey) {
    url.searchParams.delete(MAPPING_QUERY_PARAM);
  } else {
    url.searchParams.set(MAPPING_QUERY_PARAM, mappingKey);
  }

  const nextUrl = `${url.pathname}${url.search}${url.hash}`;
  if (replace) {
    window.history.replaceState(null, "", nextUrl);
    return;
  }

  window.history.pushState(null, "", nextUrl);
};
