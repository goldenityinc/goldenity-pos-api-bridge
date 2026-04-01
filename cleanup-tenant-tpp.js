// Clean Tenant Data Script for TPP tenant
// Usage: node cleanup-tenant-tpp.js

const { PrismaClient } = require("@prisma/client");

const prisma = new PrismaClient({
  datasources: {
    db: {
      url: process.env.DATABASE_URL,
    },
  },
});

const TENANT_SLUG = "tpp";

async function cleanupTenantData() {
  try {
    console.log("===============================================================================");
    console.log("TENANT DATA CLEANUP SCRIPT - " + TENANT_SLUG);
    console.log("===============================================================================\n");

    // Step 1: Find tenant
    console.log("STEP 1: Finding tenant with slug '" + TENANT_SLUG + "'...");
    const tenant = await prisma.tenant.findUnique({
      where: { slug: TENANT_SLUG },
    });

    if (!tenant) {
      console.log(
        "[ERROR] Tenant with slug '" + TENANT_SLUG + "' not found!\n"
      );
      const allTenants = await prisma.tenant.findMany({
        select: { id: true, slug: true, name: true },
        take: 10,
      });
      console.log("Available tenants:");
      allTenants.forEach((t) => {
        console.log("  - " + t.slug + " (" + t.name + ")");
      });
      process.exit(1);
    }

    console.log("[OK] Found tenant: " + tenant.slug + " (ID: " + tenant.id + ")\n");

    // Step 2: Count records using raw SQL
    console.log("STEP 2: Counting records to be deleted...\n");

    const countResult = await prisma.$queryRaw`
      SELECT 
        (SELECT COUNT(*) FROM products WHERE tenant_id = ${tenant.id}) as products,
        (SELECT COUNT(*) FROM categories WHERE tenant_id = ${tenant.id}) as categories,
        (SELECT COUNT(*) FROM customers WHERE tenant_id = ${tenant.id}) as customers,
        (SELECT COUNT(*) FROM suppliers WHERE tenant_id = ${tenant.id}) as suppliers,
        (SELECT COUNT(*) FROM transactions WHERE tenant_id = ${tenant.id}) as transactions,
        (SELECT COUNT(*) FROM sales_records WHERE tenant_id = ${tenant.id}) as sales_records,
        (SELECT COUNT(*) FROM sales_record_items WHERE tenant_id = ${tenant.id}) as sales_record_items,
        (SELECT COUNT(*) FROM order_history WHERE tenant_id = ${tenant.id}) as order_history,
        (SELECT COUNT(*) FROM order_history_items WHERE tenant_id = ${tenant.id}) as order_history_items,
        (SELECT COUNT(*) FROM restock_history WHERE tenant_id = ${tenant.id}) as restock_history,
        (SELECT COUNT(*) FROM service_orders WHERE tenant_id = ${tenant.id}) as service_orders,
        (SELECT COUNT(*) FROM store_settings WHERE tenant_id = ${tenant.id}) as store_settings,
        (SELECT COUNT(*) FROM daily_cash WHERE tenant_id = ${tenant.id}) as daily_cash,
        (SELECT COUNT(*) FROM petty_cash_logs WHERE tenant_id = ${tenant.id}) as petty_cash_logs,
        (SELECT COUNT(*) FROM expenses WHERE tenant_id = ${tenant.id}) as expenses,
        (SELECT COUNT(*) FROM kas_bon WHERE tenant_id = ${tenant.id}) as cash_bons,
        (SELECT COUNT(*) FROM custom_roles WHERE tenant_id = ${tenant.id}) as custom_roles,
        (SELECT COUNT(*) FROM app_users WHERE tenant_id = ${tenant.id}) as app_users
    `;

    const counts = countResult[0];

    let totalRecords = 0;
    Object.entries(counts).forEach(([table, countVal]) => {
      const count = Number(countVal);
      if (count > 0) {
        console.log("  * " + table + ": " + count + " records");
        totalRecords += count;
      }
    });

    console.log("\nTOTAL RECORDS TO DELETE: " + totalRecords + "\n");

    if (totalRecords === 0) {
      console.log(
        "[WARNING] No data found for tenant '" + TENANT_SLUG +
          "'. Database already clean.\n"
      );
      process.exit(0);
    }

    // Step 3: Ask for confirmation
    console.log(
      "[WARNING] This will PERMANENTLY DELETE " + totalRecords +
        " records from tenant!\n"
    );
    console.log("To confirm, run with environment variable: DELETE_CONFIRM=yes\n");

    if (process.env.DELETE_CONFIRM !== "yes") {
      console.log("[INFO] Use: DELETE_CONFIRM=yes node cleanup-tenant-tpp.js\n");
      process.exit(0);
    }

    // Step 4: Delete records in correct order (respecting FK constraints)
    console.log("STEP 3: Deleting records...\n");

    const deleteOperations = [
      {
        name: "sales_record_items",
        fn: () =>
          prisma.$executeRaw`DELETE FROM sales_record_items WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "order_history_items",
        fn: () =>
          prisma.$executeRaw`DELETE FROM order_history_items WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "sales_records",
        fn: () =>
          prisma.$executeRaw`DELETE FROM sales_records WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "transactions",
        fn: () =>
          prisma.$executeRaw`DELETE FROM transactions WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "order_history",
        fn: () =>
          prisma.$executeRaw`DELETE FROM order_history WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "restock_history",
        fn: () =>
          prisma.$executeRaw`DELETE FROM restock_history WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "service_orders",
        fn: () =>
          prisma.$executeRaw`DELETE FROM service_orders WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "products",
        fn: () =>
          prisma.$executeRaw`DELETE FROM products WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "categories",
        fn: () =>
          prisma.$executeRaw`DELETE FROM categories WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "customers",
        fn: () =>
          prisma.$executeRaw`DELETE FROM customers WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "suppliers",
        fn: () =>
          prisma.$executeRaw`DELETE FROM suppliers WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "expenses",
        fn: () =>
          prisma.$executeRaw`DELETE FROM expenses WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "store_settings",
        fn: () =>
          prisma.$executeRaw`DELETE FROM store_settings WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "daily_cash",
        fn: () =>
          prisma.$executeRaw`DELETE FROM daily_cash WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "petty_cash_logs",
        fn: () =>
          prisma.$executeRaw`DELETE FROM petty_cash_logs WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "cash_bons",
        fn: () =>
          prisma.$executeRaw`DELETE FROM kas_bon WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "custom_roles",
        fn: () =>
          prisma.$executeRaw`DELETE FROM custom_roles WHERE tenant_id = ${tenant.id}`,
      },
      {
        name: "app_users",
        fn: () =>
          prisma.$executeRaw`DELETE FROM app_users WHERE tenant_id = ${tenant.id}`,
      },
    ];

    let deletedTotal = 0;
    for (const operation of deleteOperations) {
      try {
        const deletedCount = await operation.fn();
        if (deletedCount > 0) {
          console.log("[OK] Deleted " + deletedCount + " from " + operation.name);
          deletedTotal += deletedCount;
        }
      } catch (error) {
        console.log(
          "[WARNING] Could not delete from " + operation.name + ": " +
            error.message
        );
      }
    }

    console.log("\n[OK] Total records deleted: " + deletedTotal + "\n");

    // Step 5: Verify cleanup
    console.log("STEP 4: Verifying cleanup...\n");

    const verifyResult = await prisma.$queryRaw`
      SELECT 
        (SELECT COUNT(*) FROM products WHERE tenant_id = ${tenant.id}) as products,
        (SELECT COUNT(*) FROM sales_records WHERE tenant_id = ${tenant.id}) as sales_records,
        (SELECT COUNT(*) FROM customers WHERE tenant_id = ${tenant.id}) as customers,
        (SELECT COUNT(*) FROM app_users WHERE tenant_id = ${tenant.id}) as app_users
    `;

    const verificationCounts = verifyResult[0];

    let allClean = true;
    Object.entries(verificationCounts).forEach(([table, countVal]) => {
      const count = Number(countVal);
      if (count > 0) {
        console.log("[ERROR] " + count + " records remaining in " + table);
        allClean = false;
      }
    });

    if (allClean) {
      console.log(
        "[OK] Tenant '" + TENANT_SLUG +
          "' is now clean - all data deleted"
      );
      console.log(
        "[OK] Tenant record preserved - ready for fresh testing\n"
      );
    } else {
      console.log("[WARNING] Some data may still remain\n");
    }

    console.log("===============================================================================");
    console.log("CLEANUP COMPLETE");
    console.log("===============================================================================\n");

    process.exit(0);
  } catch (error) {
    console.error("[ERROR] Fatal error:", error);
    process.exit(1);
  } finally {
    await prisma.$disconnect();
  }
}

cleanupTenantData();
