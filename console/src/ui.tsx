import * as Dialog from "@radix-ui/react-dialog";
import * as Select from "@radix-ui/react-select";
import * as Tooltip from "@radix-ui/react-tooltip";
import { Check, ChevronDown, X } from "lucide-react";
import type { ButtonHTMLAttributes, HTMLAttributes, ReactNode } from "react";
import { cn } from "./lib";

export function Button({ className, variant = "primary", ...props }: ButtonHTMLAttributes<HTMLButtonElement> & { variant?: "primary" | "secondary" | "ghost" }) {
  return <button className={cn("button", `button-${variant}`, className)} {...props} />;
}

export function Badge({ status, children }: { status?: string; children: ReactNode }) {
  return <span className={cn("badge", status && `badge-${status.toLowerCase()}`)}><i />{children}</span>;
}

export function Card({ className, ...props }: HTMLAttributes<HTMLDivElement>) {
  return <div className={cn("card", className)} {...props} />;
}

export function Hint({ label, children }: { label: string; children: ReactNode }) {
  return <Tooltip.Provider delayDuration={250}><Tooltip.Root><Tooltip.Trigger asChild>{children}</Tooltip.Trigger><Tooltip.Portal><Tooltip.Content className="tooltip" sideOffset={8}>{label}<Tooltip.Arrow className="tooltip-arrow" /></Tooltip.Content></Tooltip.Portal></Tooltip.Root></Tooltip.Provider>;
}

export function SelectField({ value, onValueChange, options, label }: { value: string; onValueChange: (value: string) => void; options: { value: string; label: string }[]; label: string }) {
  return <Select.Root value={value} onValueChange={onValueChange}>
    <Select.Trigger className="select-trigger" aria-label={label}><Select.Value /><Select.Icon><ChevronDown size={14} /></Select.Icon></Select.Trigger>
    <Select.Portal><Select.Content className="select-content" position="popper" sideOffset={5}><Select.Viewport>{options.map((option) => <Select.Item className="select-item" value={option.value} key={option.value}><Select.ItemText>{option.label}</Select.ItemText><Select.ItemIndicator><Check size={13} /></Select.ItemIndicator></Select.Item>)}</Select.Viewport></Select.Content></Select.Portal>
  </Select.Root>;
}

export function Sheet({ open, onOpenChange, title, description, children }: { open: boolean; onOpenChange: (open: boolean) => void; title: string; description?: string; children: ReactNode }) {
  return <Dialog.Root open={open} onOpenChange={onOpenChange}><Dialog.Portal><Dialog.Overlay className="sheet-overlay" /><Dialog.Content className="sheet-content"><div className="sheet-header"><div><Dialog.Title>{title}</Dialog.Title>{description && <Dialog.Description>{description}</Dialog.Description>}</div><Dialog.Close className="icon-button" aria-label="Close"><X size={18} /></Dialog.Close></div>{children}</Dialog.Content></Dialog.Portal></Dialog.Root>;
}
