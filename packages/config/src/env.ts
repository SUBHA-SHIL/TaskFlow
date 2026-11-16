export function getRequiredEnv(name: string): string {
  const value = process.env[name];

  if (!value?.trim()) {
    throw new Error(`${name} environment variable is required`);
  }

  return value;
}

export function getPositiveInteger(
  name: string,
  defaultValue: number,
): number {
  const value = process.env[name];

  if (value === undefined) {
    return defaultValue;
  }

  const parsed = Number(value);

  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }

  return parsed;
}
