package com.cedarsoftware.ncube.rules.examples;

import com.cedarsoftware.ncube.NCube;
import com.cedarsoftware.ncube.NCubeAppContext;
import com.cedarsoftware.ncube.rules.BusinessRule;
import com.cedarsoftware.ncube.rules.Documentation;
import com.cedarsoftware.util.CompactCILinkedMap;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

@SuppressWarnings("unused")
public class Transform extends BusinessRule
{
    private Map<String, Object> policy;

    @SuppressWarnings("unchecked")
    public Transform(Map<String, Object> root)
    {
        super(root);
        policy = (Map<String, Object>) root.get("policy");
    }

    @Documentation("Set expiration date to 1 year after the effective date")
    void setExpirationDate()
    {
        Date effectiveDate = safeParseDate(policy.get("effectiveDate"), "POL_DATE_FORMAT", "Policy effectiveDate is an invalid date format.");
        Calendar c = Calendar.getInstance();
        c.setTime(effectiveDate);
        c.add(Calendar.YEAR, 1);
        Date expirationDate = c.getTime();
        String expirationDateString = new SimpleDateFormat("yyyy-mm-dd").format(expirationDate);
        policy.put("expirationDate", expirationDateString);
    }

    @Documentation(value = "Set the company size based on the number of employees", ncubes = "lookup.company.size")
    void deriveCompanySize()
    {
        NCube sizeLookup = NCubeAppContext.getNcubeRuntime().getCube(getAppId(), "lookup.company.size");
        Map<String, Object> coord = new CompactCILinkedMap<>();
        coord.put("numberOfEmployees", policy.get("numberOfEmployees"));
        String size = (String) sizeLookup.getCell(coord);
        policy.put("companySize", size);
    }

}
