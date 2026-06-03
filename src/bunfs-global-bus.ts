export type BunfsGlobalBusCandidate = {
  exportName: string;
  specifier: string;
  localName: string;
};

export function findBunfsCandidates(
  binaryText: string,
): BunfsGlobalBusCandidate[] {
  const candidates: BunfsGlobalBusCandidate[] = [];
  const importPattern =
    /import\{([^}]+)\}from"(\/\$bunfs\/root\/chunk-[^"]+\.js)"/g;
  const compact = binaryText.split("\0").join("");

  for (const match of compact.matchAll(importPattern)) {
    const clause = match[1];
    const specifier = match[2];
    const importStart = match.index ?? 0;

    if (!clause || !specifier) {
      continue;
    }

    const segment = compact.slice(importStart, importStart + 200_000);

    for (const binding of readImportBindings(clause)) {
      if (!usesGlobalEventBus(segment, binding.localName)) {
        continue;
      }

      candidates.push({
        exportName: binding.exportName,
        localName: binding.localName,
        specifier,
      });
    }
  }

  return dedupeBunfsCandidates(candidates);
}

function readImportBindings(clause: string) {
  return clause
    .split(",")
    .map((part) => part.trim())
    .flatMap((part) => {
      if (!part) {
        return [];
      }

      const alias = part.match(
        /^([A-Za-z_$][\w$]*)\s+as\s+([A-Za-z_$][\w$]*)$/,
      );

      if (alias?.[1] && alias[2]) {
        return [{ exportName: alias[1], localName: alias[2] }];
      }

      return [{ exportName: part, localName: part }];
    });
}

function dedupeBunfsCandidates(candidates: BunfsGlobalBusCandidate[]) {
  const seen = new Set<string>();
  const result: BunfsGlobalBusCandidate[] = [];

  for (const candidate of candidates) {
    const key = `${candidate.specifier}:${candidate.exportName}`;

    if (seen.has(key)) {
      continue;
    }

    seen.add(key);
    result.push(candidate);
  }

  return result;
}

function usesGlobalEventBus(segment: string, localName: string) {
  return (
    segment.includes(`${localName}.on("event"`) ||
    segment.includes(`${localName}.emit("event"`)
  );
}
