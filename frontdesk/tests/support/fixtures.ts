/**
 * Shared test data: tenants built through the same schema as real tenant files.
 */

import type { Tenant } from "../../src/core/index.ts";
import { parseTenant, type TenantFile } from "../../src/tenants/schema.ts";

export const DEALER_NUMBER  = "+13175550100";
export const PLUMBER_NUMBER = "+13175550200";
export const CUSTOMER       = "+13175550111";

export function tenant(overrides: Partial<TenantFile> & Pick<TenantFile, "id" | "slug" | "domain">): Tenant {
    const result = parseTenant({
        name:      overrides.slug,
        timezone:  "America/Indiana/Indianapolis",
        hoursText: "Mon-Fri 8am-6pm, Sat 9am-2pm, Sun closed",
        ...overrides,
    });
    if (!result.ok) {
        throw new Error(JSON.stringify(result.problems));
    }
    return result.tenant;
}

export function plumber(overrides: Partial<TenantFile> = {}): Tenant {
    return tenant({
        id:        "plumber",
        slug:      "demo-plumber",
        domain:    "plumbing",
        name:      "Circle City Plumbing",
        phone:     "317-555-0299",
        endpoints: { sms: [PLUMBER_NUMBER], voice: [PLUMBER_NUMBER] },
        staff:     { phones: ["+13175550888"], emails: ["owner@circlecity.example"] },
        profile:   { service_area: "Indianapolis and the donut counties" },
        ...overrides,
    });
}
