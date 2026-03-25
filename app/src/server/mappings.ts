export type MappingsPayload = {
  [key: string]: { [key: string]: { [key: string]: string } };
};

const MAPPINGS_URL =
  "https://github.com/anibridge/anibridge-mappings/releases/latest/download/mappings.json";
const LOCAL_MAPPINGS_FS_PATH = `/@fs${new URL(
  "../../../data/out/mappings.json",
  import.meta.url,
).pathname}`;

let mappingsPromise: Promise<MappingsPayload> | null = null;

export const getMappings = async (requestUrl?: string): Promise<MappingsPayload> => {
  const EDGE_CACHE_TTL = 6 * 60 * 60;

  if (!mappingsPromise) {
    mappingsPromise = (async () => {
      if (import.meta.env.DEV) {
        if (!requestUrl) {
          throw new Error("requestUrl is required in DEV to resolve local mappings.");
        }
        const localUrl = new URL(LOCAL_MAPPINGS_FS_PATH, requestUrl);
        const res = await fetch(localUrl.toString(), {
          headers: { Accept: "application/json" },
        });
        if (!res.ok) {
          throw new Error(
            `Failed to fetch local mappings: ${res.status} ${res.statusText}`,
          );
        }
        return (await res.json()) as MappingsPayload;
      }

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
