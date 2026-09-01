import { clsx, type ClassValue } from "clsx";
import { formatDistanceToNowStrict } from "date-fns";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function relativeTime(seconds?: number) {
  return seconds ? formatDistanceToNowStrict(new Date(seconds * 1000), { addSuffix: true }) : "—";
}

export function duration(seconds = 0) {
  if (seconds < 1) return `${Math.max(0, seconds * 1000).toFixed(0)} ms`;
  if (seconds < 60) return `${seconds.toFixed(2)} s`;
  return `${Math.floor(seconds / 60)}m ${Math.floor(seconds % 60)}s`;
}

export function compact(value = 0) {
  return Intl.NumberFormat("en", { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

export function title(value = "") {
  return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}
