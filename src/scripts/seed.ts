import { db, usersTable, leadsTable, documentsTable, callsTable, dealsTable } from "../configs/database";

async function seed() {
  console.log("Seeding database...");

  const [admin] = await db.insert(usersTable).values({
    email: "admin@cashadvance.com",
    password: "admin123",
    fullName: "Admin Owner",
    role: "admin",
    phone: "(555) 100-0000",
  }).onConflictDoNothing().returning();

  const [rep1] = await db.insert(usersTable).values({
    email: "john@cashadvance.com",
    password: "rep123",
    fullName: "John Martinez",
    role: "rep",
    phone: "(555) 200-0001",
  }).onConflictDoNothing().returning();

  const [rep2] = await db.insert(usersTable).values({
    email: "sarah@cashadvance.com",
    password: "rep123",
    fullName: "Sarah Johnson",
    role: "rep",
    phone: "(555) 200-0002",
  }).onConflictDoNothing().returning();

  if (!admin || !rep1 || !rep2) {
    console.log("Users already exist, skipping seed.");
    process.exit(0);
  }

  const leads = await db.insert(leadsTable).values([
    {
      businessName: "Mike's Auto Repair",
      ownerName: "Mike Thompson",
      email: "mike@mikesauto.com",
      phone: "(555) 301-1001",
      status: "new",
      assignedToId: rep1.id,
      requestedAmount: "75000",
      monthlyRevenue: "45000",
      industry: "Automotive",
      address: "123 Main St",
      city: "Dallas",
      state: "TX",
      zip: "75201",
      ein: "12-3456789",
      yearsInBusiness: "8",
    },
    {
      businessName: "Fresh Bites Restaurant",
      ownerName: "Maria Garcia",
      email: "maria@freshbites.com",
      phone: "(555) 301-1002",
      status: "new",
      assignedToId: rep1.id,
      requestedAmount: "50000",
      monthlyRevenue: "60000",
      industry: "Food & Beverage",
      address: "456 Oak Ave",
      city: "Houston",
      state: "TX",
      zip: "77002",
      ein: "98-7654321",
      yearsInBusiness: "3",
    },
    {
      businessName: "Elite Construction LLC",
      ownerName: "Robert Williams",
      email: "robert@eliteconstruct.com",
      phone: "(555) 301-1003",
      status: "qualified",
      assignedToId: rep1.id,
      requestedAmount: "150000",
      monthlyRevenue: "120000",
      industry: "Construction",
      address: "789 Industrial Blvd",
      city: "Austin",
      state: "TX",
      zip: "73301",
      ein: "45-6789012",
      yearsInBusiness: "12",
      lastContactedAt: new Date("2026-03-10"),
    },
    {
      businessName: "Sunshine Daycare",
      ownerName: "Lisa Chen",
      email: "lisa@sunshinedaycare.com",
      phone: "(555) 301-1004",
      status: "new",
      assignedToId: rep2.id,
      requestedAmount: "35000",
      monthlyRevenue: "28000",
      industry: "Childcare",
      address: "321 Maple Dr",
      city: "San Antonio",
      state: "TX",
      zip: "78201",
      ein: "67-8901234",
      yearsInBusiness: "5",
    },
    {
      businessName: "Pro Plumbing Services",
      ownerName: "David Brown",
      email: "david@proplumbing.com",
      phone: "(555) 301-1005",
      status: "contacted",
      assignedToId: rep2.id,
      requestedAmount: "60000",
      monthlyRevenue: "55000",
      industry: "Plumbing",
      address: "654 Elm St",
      city: "Fort Worth",
      state: "TX",
      zip: "76101",
      ein: "23-4567890",
      yearsInBusiness: "7",
      lastContactedAt: new Date("2026-03-12"),
    },
    {
      businessName: "Bella's Boutique",
      ownerName: "Isabella Rossi",
      email: "bella@bellasboutique.com",
      phone: "(555) 301-1006",
      status: "no_answer",
      assignedToId: rep2.id,
      requestedAmount: "25000",
      monthlyRevenue: "18000",
      industry: "Retail",
      address: "987 Fashion Ave",
      city: "Plano",
      state: "TX",
      zip: "75024",
      yearsInBusiness: "2",
      lastContactedAt: new Date("2026-03-11"),
    },
    {
      businessName: "TechStart Solutions",
      ownerName: "James Park",
      email: "james@techstart.io",
      phone: "(555) 301-1007",
      status: "new",
      assignedToId: rep1.id,
      requestedAmount: "100000",
      monthlyRevenue: "85000",
      industry: "Technology",
      address: "111 Tech Blvd",
      city: "Irving",
      state: "TX",
      zip: "75038",
      ein: "34-5678901",
      yearsInBusiness: "4",
    },
    {
      businessName: "Green Landscaping Co",
      ownerName: "Carlos Mendez",
      email: "carlos@greenlandscaping.com",
      phone: "(555) 301-1008",
      status: "callback",
      assignedToId: rep1.id,
      requestedAmount: "40000",
      monthlyRevenue: "32000",
      industry: "Landscaping",
      address: "222 Garden Ln",
      city: "Arlington",
      state: "TX",
      zip: "76010",
      yearsInBusiness: "6",
      lastContactedAt: new Date("2026-03-12"),
    },
  ]).returning();

  for (const lead of leads) {
    await db.insert(documentsTable).values([
      {
        leadId: lead.id,
        type: "bank_statement",
        name: "Bank Statement - Jan 2026",
        url: "https://placehold.co/600x800/1a1a2e/eaeaea?text=Bank+Statement",
      },
      {
        leadId: lead.id,
        type: "id_document",
        name: "Driver's License",
        url: "https://placehold.co/600x400/1a1a2e/eaeaea?text=ID+Document",
      },
      {
        leadId: lead.id,
        type: "void_check",
        name: "Void Check",
        url: "https://placehold.co/600x300/1a1a2e/eaeaea?text=Void+Check",
      },
    ]);
  }

  const qualifiedLead = leads[2];
  const contactedLead = leads[4];

  await db.insert(callsTable).values([
    {
      leadId: qualifiedLead.id,
      userId: rep1.id,
      outcome: "interested",
      notes: "Owner very interested, wants to discuss terms. Has strong revenue.",
      duration: 320,
    },
    {
      leadId: contactedLead.id,
      userId: rep2.id,
      outcome: "callback",
      notes: "Owner busy, asked to call back Friday afternoon.",
      duration: 45,
      callbackAt: new Date("2026-03-14T15:00:00"),
    },
    {
      leadId: leads[5].id,
      userId: rep2.id,
      outcome: "no_answer",
      notes: null,
      duration: null,
    },
    {
      leadId: leads[7].id,
      userId: rep1.id,
      outcome: "callback",
      notes: "Left voicemail, owner texted back - will call tomorrow.",
      duration: 15,
      callbackAt: new Date("2026-03-14T10:00:00"),
    },
  ]);

  await db.insert(dealsTable).values([
    {
      leadId: qualifiedLead.id,
      repId: rep1.id,
      stage: "underwriting",
      amount: "150000",
      factorRate: "1.35",
      paybackAmount: "202500",
      term: 12,
      commission: "4500",
    },
    {
      leadId: contactedLead.id,
      repId: rep2.id,
      stage: "prospect",
      amount: "60000",
      factorRate: "1.40",
    },
  ]);

  console.log("Seed complete!");
  console.log(`Admin login: admin@cashadvance.com / admin123`);
  console.log(`Rep 1 login: john@cashadvance.com / rep123`);
  console.log(`Rep 2 login: sarah@cashadvance.com / rep123`);
  process.exit(0);
}

seed().catch(console.error);
