export type MappingsPayload = {
  [key: string]: { [key: string]: { [key: string]: string } };
};

let mappingsPromise: Promise<MappingsPayload> | null = null;

export const getMappings = async (): Promise<MappingsPayload> => {
  if (!mappingsPromise) {
    mappingsPromise = import("../../../data/out/mappings.min.json").then(
      (module) => module.default as MappingsPayload,
    );
  }
  return mappingsPromise;
};
