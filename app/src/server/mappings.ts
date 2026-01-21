export type MappingsPayload = {
  [key: string]: { [key: string]: { [key: string]: string } };
};

const MAPPINGS_URL =
  "https://github.com/anibridge/anibridge-mappings/releases/latest/download/mappings.json";

let mappingsPromise: Promise<MappingsPayload> | null = null;

export const getMappings = async (): Promise<MappingsPayload> => {
  const EDGE_CACHE_TTL = 6 * 60 * 60;

  if (!mappingsPromise) {
    mappingsPromise = (async () => {
      const init: any = {
        headers: { Accept: "application/json" },
        cf: { cacheTtl: EDGE_CACHE_TTL, cacheEverything: true },
      };
      const res = await fetch(MAPPINGS_URL, init);
      if (!res.ok) {
        throw new Error(
          `Failed to fetch mappings: ${res.status} ${res.statusText}`,
        );
      }
      return (await res.json()) as MappingsPayload;
    })().catch((err) => {
      mappingsPromise = null;
      throw err;
    });
  }

  return mappingsPromise;
};
