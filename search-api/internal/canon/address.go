package canon

import (
    "regexp"
    "strings"
)

var rePunct = regexp.MustCompile(`[^A-Za-z0-9\s]`)

// Canonicalize normalizes an address and computes a stable property key.
// It intentionally ignores unit/suite to stabilize identity per parcel.
func Canonicalize(line1, city, state, zip string) (normLine1, normCity, normState, normZip, propertyKey string) {
    n1 := strings.TrimSpace(strings.ToUpper(line1))
    n1 = stripUnit(n1)
    n1 = rePunct.ReplaceAllString(n1, " ")
    n1 = abbreviateSuffix(n1)
    n1 = collapseSpaces(n1)

    c := collapseSpaces(rePunct.ReplaceAllString(strings.ToUpper(strings.TrimSpace(city)), " "))
    st := strings.ToUpper(strings.TrimSpace(state))
    if len(st) > 2 { st = stateAbbrev(st) }
    z := trimZIP(zip)

    key := strings.ToLower(n1 + "|" + c + "|" + st + "|" + z)
    return n1, c, st, z, key
}

func collapseSpaces(s string) string {
    return strings.Join(strings.Fields(s), " ")
}

func trimZIP(z string) string {
    z = strings.TrimSpace(z)
    if len(z) >= 5 { return z[:5] }
    return z
}

func stripUnit(s string) string {
    // Remove trailing unit designators like APT, UNIT, STE, SUITE, #
    toks := []string{" APT ", " UNIT ", " STE ", " SUITE ", " #"}
    up := " " + s + " "
    for _, t := range toks {
        if i := strings.Index(up, t); i >= 0 {
            return strings.TrimSpace(up[:i])
        }
    }
    return strings.TrimSpace(s)
}

func abbreviateSuffix(s string) string {
    // Basic USPS-style suffix normalization
    repl := map[string]string{
        " STREET": " ST",
        " ROAD": " RD",
        " AVENUE": " AVE",
        " BOULEVARD": " BLVD",
        " DRIVE": " DR",
        " LANE": " LN",
        " COURT": " CT",
        " CIRCLE": " CIR",
        " TERRACE": " TER",
        " PLACE": " PL",
        " PARKWAY": " PKWY",
        " HIGHWAY": " HWY",
    }
    out := s
    for k, v := range repl { out = strings.ReplaceAll(out, k, v) }
    return out
}

func stateAbbrev(s string) string {
    m := map[string]string{
        "ALABAMA":"AL","ALASKA":"AK","ARIZONA":"AZ","ARKANSAS":"AR","CALIFORNIA":"CA","COLORADO":"CO","CONNECTICUT":"CT","DELAWARE":"DE","FLORIDA":"FL","GEORGIA":"GA","HAWAII":"HI","IDAHO":"ID","ILLINOIS":"IL","INDIANA":"IN","IOWA":"IA","KANSAS":"KS","KENTUCKY":"KY","LOUISIANA":"LA","MAINE":"ME","MARYLAND":"MD","MASSACHUSETTS":"MA","MICHIGAN":"MI","MINNESOTA":"MN","MISSISSIPPI":"MS","MISSOURI":"MO","MONTANA":"MT","NEBRASKA":"NE","NEVADA":"NV","NEW HAMPSHIRE":"NH","NEW JERSEY":"NJ","NEW MEXICO":"NM","NEW YORK":"NY","NORTH CAROLINA":"NC","NORTH DAKOTA":"ND","OHIO":"OH","OKLAHOMA":"OK","OREGON":"OR","PENNSYLVANIA":"PA","RHODE ISLAND":"RI","SOUTH CAROLINA":"SC","SOUTH DAKOTA":"SD","TENNESSEE":"TN","TEXAS":"TX","UTAH":"UT","VERMONT":"VT","VIRGINIA":"VA","WASHINGTON":"WA","WEST VIRGINIA":"WV","WISCONSIN":"WI","WYOMING":"WY",
    }
    if v, ok := m[s]; ok { return v }
    return s
}

