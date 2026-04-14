export interface BankTemplate {
  name: string;
  aliases: string[];
  identifyPattern: RegExp;
  dateFormats: RegExp[];
  sectionHeaders: {
    deposits: RegExp;
    withdrawals: RegExp;
    checks?: RegExp;
  };
  summaryPatterns: {
    totalDeposits: RegExp[];
    totalWithdrawals: RegExp[];
  };
  transactionLinePatterns: RegExp[];
  columnLayout: "single" | "dual" | "auto";
  creditColumn?: "left" | "right";
  debitColumn?: "left" | "right";
  specialRules?: string[];
  notes?: string;
  useDetailedDepositTotal?: boolean;
}

const BANK_TEMPLATES: BankTemplate[] = [
  {
    name: "First International Bank & Trust",
    aliases: ["First International Bank & Trust", "FIBT"],
    identifyPattern: /\bFIBT\.com\b|\bFirst\s+International\s+Bank\s+&\s+Trust\b/i,
    dateFormats: [/(\d{1,2}\/\d{1,2}\/\d{2,4})/, /(\d{1,2}\/\d{1,2})/],
    sectionHeaders: {
      deposits: /Account\s+Summary/i,
      withdrawals: /Account\s+Summary/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /(\d+)\s+Credit\(s\)\s+This\s+Period\$?\s*([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [
        /(\d+)\s+Debit\(s\)\s+This\s+Period\$?\s*([\d,]+\.\d{2})/i,
      ],
    },
    transactionLinePatterns: [
      /^(\d{1,2}\/\d{1,2}\/\d{2,4})\s+(.+?)\s+\$?([\d,]+\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "Eastern Michigan Bank",
    aliases: ["Eastern Michigan Bank", "EASTERN MICHIGAN BANK"],
    identifyPattern: /\bXXXXXXXX\d{4}\b/i,
    dateFormats: [/(\d{1,2}\/\d{1,2}\/\d{2,4})/, /(\d{1,2}\/\d{1,2})/],
    sectionHeaders: {
      deposits: /DEPOSITS\s+AND\s+ADDITIONS/i,
      withdrawals: /CHECKS\s+AND\s+WITHDRAWALS/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /\b(\d+)\s+Deposits?\/Credits\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [
        /\b(\d+)\s+Checks?\/Debits\s+\$?([\d,]+\.\d{2})/i,
      ],
    },
    transactionLinePatterns: [
       /^(\d{1,2}\/\d{2})\s+([A-Z].+?)\s+([\d,]+\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "Chase",
    aliases: ["JPMorgan Chase", "Chase Bank", "JPMORGAN CHASE"],
    identifyPattern: /\bJPMorgan\s+Chase\b|\bChase\s+Bank\b|\bCHASE\s+(?:BANK|CREDIT|CHECKING|SAVINGS|BUSINESS|ACCOUNT|STATEMENT)\b/i,
    dateFormats: [/(\d{2}\/\d{2}(?:\/\d{2,4})?)/, /(\d{2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS\s+AND\s+(?:OTHER\s+)?(?:ADDITIONS|CREDITS)/i,
      withdrawals: /(?:ELECTRONIC\s+(?:WITHDRAWALS?|PAYMENTS?)|CHECKS?\s+(?:AND\s+OTHER\s+)?(?:DEBITS?|WITHDRAWALS?)|(?:ATM\s+&?\s*)?DEBIT\s+CARD\s+(?:WITHDRAWALS?|PURCHASES?))/i,
      checks: /CHECKS?\s+PAID/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /(?:Deposits\s+and\s+(?:Other\s+)?(?:Additions|Credits))\s+(\d+)\s+\$?([\d,]+\.\d{2})/i,
        /DEPOSITS\s+AND\s+ADDITIONS\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [
        /(?:Electronic\s+Withdrawals?|ATM\s+&\s+Debit\s+Card|Checks?\s+Paid)\s+\d+\s+\$?([\d,]+\.\d{2})/i,
      ],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2}(?:\/\d{2,4})?)\s+(.+?)\s+(-?\$?[\d,]+\.\d{2})\s*$/,
      /^(\d{2}\/\d{2}(?:\/\d{2,4})?)\s+(.+?)\s+(-?\$?[\d,]+\.\d{2})\s+(-?\$?[\d,]+\.\d{2})\s*$/,
    ],
    columnLayout: "single",
    specialRules: ["chase_summary_table"],
  },
  {
    name: "Bank of America",
    aliases: ["BofA", "BANK OF AMERICA"],
    identifyPattern: /\bBank\s+of\s+America\b|\bBofA\b|\bBANK\s+OF\s+AMERICA/i,
    dateFormats: [/(\d{2}\/\d{2}\/\d{2})/, /(\d{2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS\s+AND\s+OTHER\s+CREDITS|TOTAL\s+ADDITIONS/i,
      withdrawals: /(?:CHECKS?\s+PAID|WITHDRAWALS?\s+AND\s+(?:OTHER\s+)?DEBITS?|ELECTRONIC\s+TRANSACTIONS?|OTHER\s+SUBTRACTIONS?|TOTAL\s+SUBTRACTIONS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /(\d+)\s+Deposits?\s+and\s+(?:Other\s+)?Credits?\s+\$?([\d,]+\.\d{2})/i,
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
        /Total\s+additions?\s+\$?([\d,]+\.\d{2})/i,
        /Total\s+additions?\s+([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [
        /Total\s+(?:Withdrawals?\s+and\s+(?:Other\s+)?Debits?|Checks?\s+Paid)\s+\$?([\d,]+\.\d{2})/i,
        /Total\s+subtractions?\s+\$?([\d,]+\.\d{2})/i,
        /Total\s+subtractions?\s+([\d,]+\.\d{2})/i,
      ],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "dual",
    creditColumn: "left",
    debitColumn: "left",
  },
  {
    name: "Wells Fargo",
    aliases: ["Wells Fargo"],
    identifyPattern: /\bWells\s+Fargo\b/i,
    dateFormats: [/(\d{1,2}\/\d{1,2})/],
    sectionHeaders: {
      deposits: /(?:DEPOSITS|CREDITS?\s+AND\s+DEPOSITS?|TRANSACTION\s+HISTORY)/i,
      withdrawals: /(?:WITHDRAWALS?|DEBITS?\s+AND\s+(?:OTHER\s+)?WITHDRAWALS?|CHECKS?\s+PAID|DAILY\s+CARD\s+WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Deposits?\s*\/?\s*Credits?\s+\$?([\d,]+\.\d{2})/i,
        /Total\s+Deposits?\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [
        /Withdrawals?\s*\/?\s*Debits?\s+\$?([\d,]+\.\d{2})/i,
      ],
    },
    transactionLinePatterns: [
      /^(\d{1,2}\/\d{1,2})\s+(.+?)\s+(-?\$?[\d,]+\.\d{2})\s*$/,
      /^(\d{1,2}\/\d{1,2})\s+(.+?)\s+(-?\$?[\d,]+\.\d{2})\s+(-?\$?[\d,]+\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "TD Bank",
    aliases: ["TD Checking"],
    identifyPattern: /\bTD\s+Bank\b|\bTD\s+Checking\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{4})/],
    sectionHeaders: {
      deposits: /(?:DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS?|ELECTRONIC\s+DEPOSITS?)/i,
      withdrawals: /(?:ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|CHECKS?\s+(?:CASHED|PAID)|DEBITS?\s+AND\s+(?:OTHER\s+)?WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Deposits?,?\s+Credits?\s+and\s+Interest\s+\$?([\d,]+\.\d{2})/i,
        /Total\s+Deposits?\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [
        /(?:Checks|Withdrawals?|Debits?),?\s+(?:and\s+)?(?:Other\s+)?(?:Debits?|Charges?)\s+\$?([\d,]+\.\d{2})/i,
      ],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "dual",
  },
  {
    name: "PNC",
    aliases: ["PNC Bank"],
    identifyPattern: /\bPNC\s+Bank\b|\bPNC\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:BANKING\/DEBIT\s+CARD\s+WITHDRAWALS?|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|CHECKS?\s+AND\s+SUBSTITUTE\s+CHECKS?|OTHER\s+DEDUCTIONS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+(\d+)\s+\$?([\d,]+\.\d{2})/i,
        /Deposits?\s+and\s+(?:Other\s+)?Credits?\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "Capital One",
    aliases: ["Capital One"],
    identifyPattern: /\bCapital\s+One\b/i,
    dateFormats: [/(\w{3}\s+\d{1,2})/, /(\d{1,2}\/\d{1,2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /WITHDRAWALS?\s+AND\s+(?:OTHER\s+)?DEBITS/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /(\d+)\s+Deposits?\s*\/\s*Credits?\s+\$?([\d,]+\.\d{2})/i,
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{1,2}\/\d{1,2})\s+(.+?)\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s*$/,
      /^(\d{1,2}\/\d{1,2})\s+(.+?)\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s*$/,
      /^(\d{1,2}\/\d{1,2})\s+(.+?)\s+\$?([\d,]+\.\d{2})\s*$/,
      /^(\w{3}\s+\d{1,2})\s+(.+?)\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s*$/,
      /^(\w{3}\s+\d{1,2})\s+(.+?)\s+\$?([\d,]+\.\d{2})\s+\$?([\d,]+\.\d{2})\s*$/,
      /^(\w{3}\s+\d{1,2})\s+(.+?)\s+\$?([\d,]+\.\d{2})\s*$/,
    ],
    columnLayout: "single",
    notes: "Capital One 'Business Basic Checking' ACCOUNT DETAIL has 3 amount columns: Deposits/Credits | Withdrawals/Debits | Resulting Balance. Lines may have deposit in col1 OR withdrawal in col2, plus balance in col3. Account number inline: 'Business Basic Checking 00002082812044'. ACCOUNT SUMMARY shows 'N Deposits/Credits $X' format.",
  },
  {
    name: "US Bank",
    aliases: ["U.S. Bank"],
    identifyPattern: /\bUS\s+Bank\b|\bU\.S\.\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+(?:AND\s+)?(?:OTHER\s+)?CREDITS|(?:CUSTOMER|OTHER)\s+DEPOSITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|OTHER\s+WITHDRAWALS?|ELECTRONIC\s+WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Deposits?\s+and\s+(?:Other\s+)?Credits?\s+\$?\s*([\d,]+\.\d{2})/i,
        /Customer\s+Deposits?\s+\d+\s+\$?\s*([\d,]+\.\d{2})/i,
        /Other\s+Deposits?\s+\d+\s+\$?\s*([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
    notes: "Silver Business Checking: Account Summary has SEPARATE deposit categories (Customer Deposits + Other Deposits) that must be SUMMED for total deposits. No single 'Total Deposits' line exists.",
  },
  {
    name: "Regions",
    aliases: ["Regions Bank", "Regions Financial"],
    identifyPattern: /\bRegions\s+Bank\b|\bRegions\b/i,
    dateFormats: [/(\d{2}\/\d{2}\/\d{4})/, /(\d{2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:WITHDRAWALS?\s+AND\s+(?:OTHER\s+)?DEBITS?|CHECKS?\s+PRESENTED)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2}\/\d{4})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "Truist",
    aliases: ["Truist Bank", "BB&T", "SunTrust"],
    identifyPattern: /\bTruist\b|\bBB&T\b|\bSunTrust\b/i,
    dateFormats: [/(\d{2}\/\d{2}\/\d{2})/, /(\d{2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
    notes: "Truist PDFs often contain MULTIPLE accounts in a single file (e.g., BUSINESS VALUE 500 CHECKING ••2797 with small deposits alongside TRUIST DYNAMIC BUSINESS CHECKING ••6999 with main business deposits). The parser may extract deposits from the secondary/savings account. Always use the AI reading which identifies the correct main business account. Account types: 'Business Value 500 Checking' = secondary/savings, 'Dynamic Business Checking - Core Tier' = main checking.",
  },
  {
    name: "Santander",
    aliases: ["Santander Bank"],
    identifyPattern: /\bSantander\b/i,
    dateFormats: [/(\d{2}\/\d{2}\/\d{2})/, /(\d{2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?|WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /(?:Credits?\s+This\s+Period|Total\s+Deposits?)\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [
        /(?:Debits?\s+This\s+Period|Total\s+Withdrawals?)\s+\$?([\d,]+\.\d{2})/i,
      ],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "Citizens",
    aliases: ["Citizens Bank"],
    identifyPattern: /\bCitizens\s+Bank\b|\bCitizens\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+PAYMENTS?|WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "Huntington",
    aliases: ["Huntington Bank", "Huntington National"],
    identifyPattern: /\bHuntington\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+PAYMENTS?|WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "Fifth Third",
    aliases: ["Fifth Third Bank"],
    identifyPattern: /\bFifth\s+Third\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+WITHDRAWALS?|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "BMO",
    aliases: ["BMO Harris"],
    identifyPattern: /\bBMO\b|\bBMO\s+Harris\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+WITHDRAWALS?|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "KeyBank",
    aliases: ["Key Bank"],
    identifyPattern: /\bKeyBank\b|\bKey\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+WITHDRAWALS?|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "M&T Bank",
    aliases: ["M&T"],
    identifyPattern: /\bM&T\s+Bank\b|\bM\s*&\s*T\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+WITHDRAWALS?|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "Synovus",
    aliases: ["Synovus Bank"],
    identifyPattern: /\bSynovus\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+WITHDRAWALS?|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "First Interstate",
    aliases: ["First Interstate Bank", "First Interstate BancSystem"],
    identifyPattern: /\bFirst\s+Interstate\b/i,
    dateFormats: [/(\w{3}\s+\d{1,2})/, /(\d{1,2}\/\d{1,2}\/\d{4})/, /(\d{1,2}\/\d{1,2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+WITHDRAWALS?|OTHER\s+DEBITS?|WITHDRAWALS?\s+AND\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
        /Deposits?,?\s+Credits?\s+and\s+Interest\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\w{3}\s+\d{1,2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{1,2}\/\d{1,2}\/\d{4})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{1,2}\/\d{1,2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "dual",
  },
  {
    name: "Broadway Bank",
    aliases: ["Broadway National Bank"],
    identifyPattern: /\bBroadway\s+(?:National\s+)?Bank\b/i,
    dateFormats: [/(\d{1,2}\/\d{1,2}\/\d{4})/, /(\d{1,2}\/\d{1,2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+WITHDRAWALS?|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{1,2}\/\d{1,2}\/\d{4})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{1,2}\/\d{1,2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{1,2}\/\d{1,2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "dual",
  },
  {
    name: "Arvest Bank",
    aliases: ["Arvest"],
    identifyPattern: /\bArvest\s+Bank\b|\bArvest\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?|WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Fremont Bank",
    aliases: ["Fremont"],
    identifyPattern: /\bFremont\s+Bank\b|\bFremont\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?|WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Old National Bank",
    aliases: ["Old National"],
    identifyPattern: /\bOld\s+National\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?|WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Simmons Bank",
    aliases: ["Simmons"],
    identifyPattern: /\bSimmons\s+Bank\b|\bSimmons\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?|WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Ion Bank",
    aliases: ["Ion"],
    identifyPattern: /\bIon\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?|WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Beginning\s+Balance\s+Deposits\s+Interest\s+Paid[\s\S]*?(\d[\d,]*\.\d{2})\s+(\d[\d,]*\.\d{2})/i,
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
    notes: "Account Summary table: columns are Previous Date | Beginning Balance | Deposits | Interest Paid | Withdrawals | Fees | Ending Balance. Deposits value is the 2nd amount after the column headers.",
  },
  {
    name: "Northern Interstate Bank",
    aliases: ["Northern Interstate"],
    identifyPattern: /\bNorthern\s+Interstate\s+Bank\b/i,
    dateFormats: [/(\d{1,2}\/\d{1,2}\/\d{2,4})/, /(\d{1,2}\/\d{1,2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+(?:AND\s+)?(?:OTHER\s+)?CREDITS?|TOTAL\s+DEPOSITS/i,
      withdrawals: /CHECKS?\s+(?:PAID|AND\s+OTHER\s+DEBITS)|WITHDRAWALS?|OTHER\s+DEBITS/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s*\$?([\d,]+\.\d{2})/i, /Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{1,2}\/\d{1,2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Quaint Oak Bank",
    aliases: ["Quaint Oak", "QuaintOak"],
    identifyPattern: /\bQuaint\s*Oak\b|QuaintOak/i,
    dateFormats: [/(\d{1,2}\/\d{1,2}\/\d{4})/, /(\d{1,2}\/\d{1,2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS|CREDIT\(?S?\)?\s+THIS\s+PERIOD/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?|WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
        /\d+\s+Credit\(?s?\)?\s+This\s+Period\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [
        /\d+\s+Debit\(?s?\)?\s+This\s+Period\s+\$?([\d,]+\.\d{2})/i,
      ],
    },
    transactionLinePatterns: [
      /^(\d{1,2}\/\d{1,2}\/\d{4})\s+(.+?)\s+\$?(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{1,2}\/\d{1,2})\s+(.+?)\s+\$?(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "Monson Savings",
    aliases: ["Monson Savings Bank"],
    identifyPattern: /\bMonson\s+Savings\b/i,
    dateFormats: [/(\d{2}\/\d{2}\/\d{2,4})/, /(\d{2}\/\d{2})/],
    sectionHeaders: {
      deposits: /SUMMARY\s+OF\s+YOUR\s+ACTIVITY|DEPOSIT\s+AMOUNT|DEPOSITS/i,
      withdrawals: /WITHDRAWAL\s+AMOUNT|CHECKS\s+CLEARED|ATM\s*\/?\s*CHECKCARD\s+ACTIVITY/i,
      checks: /CHECKS\s+CLEARED\s+AT\s+A\s+GLANCE/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /DEPOSIT\s+AMOUNT\s*[=:–\-\+\*►→]+\s*\$?\s*([\d,]+\.?\d*)/i,
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [
        /WITHDRAWAL\s+AMOUNT\s*[=:–\-\+\*►→]+\s*\$?\s*([\d,]+\.?\d*)/i,
      ],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2}\/\d{2,4})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
    notes: "Uses DEPOSIT AMOUNT = and WITHDRAWAL AMOUNT = in summary section. ATM/DEBIT deposits listed separately.",
  },
  {
    name: "19th Bank",
    aliases: ["Nineteenth Bank", "19TH BANK"],
    identifyPattern: /\b19th\s+Bank\b|\bNineteenth\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2}\/\d{2,4})/, /(\d{2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS|DEPOSIT\s+AMOUNT/i,
      withdrawals: /WITHDRAWAL\s+AMOUNT|CHECKS?\s+PAID|(?:OTHER\s+)?DEBITS?|WITHDRAWALS?/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /DEPOSIT\s+AMOUNT\s*[=:–\-\*►→]+\s*\$?\s*([\d,]+\.?\d*)/i,
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [
        /WITHDRAWAL\s+AMOUNT\s*[=:–\-\*►→]+\s*\$?\s*([\d,]+\.?\d*)/i,
        /Total\s+(?:Withdrawals?\s+and\s+(?:Other\s+)?Debits?|Checks?\s+Paid)\s+\$?([\d,]+\.\d{2})/i,
      ],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2}\/\d{2,4})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
  {
    name: "FirstLight Federal Credit Union",
    aliases: ["FirstLight FCU", "FirstLight", "FIRSTLIGHT"],
    identifyPattern: /\bFirstLight\b|\bFirst\s*Light\s+Federal\b/i,
    dateFormats: [/(\d{2}\/\d{2}\/\d{2,4})/, /(\d{2}\/\d{2})/],
    sectionHeaders: {
      deposits: /OTHER\s+CREDITS|ATM\s*\/?\s*DEBIT.*DEPOSIT|DEPOSITS/i,
      withdrawals: /OTHER\s+DEBITS|ATM\s*\/?\s*CHECKCARD\s+ACTIVITY.*WITHDRAWAL|CHECKS\s+CLEARED/i,
      checks: /CHECKS\s+CLEARED\s+AT\s+A\s+GLANCE/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /\d+\s+Other\s+Credits?\s+(?:for:?\s*)?\$?([\d,]+\.\d{2})/i,
        /\d+\s+ATM\s*\/?\s*DEBIT\s+Deposits?:?\s*\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [
        /\d+\s+Other\s+Debits?\s+(?:for:?\s*)?\$?([\d,]+\.\d{2})/i,
        /\d+\s+ATM\s*\/?\s*DEBIT\s+Withdrawals?:?\s*\$?([\d,]+\.\d{2})/i,
      ],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2}\/\d{2,4})\s+(.+?)\s+(-?\d[\d,]*\.\d{2})\s+(-?\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2}\/\d{2,4})\s+(.+?)\s+(-?\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "dual",
    creditColumn: "right",
    debitColumn: "left",
    notes: "Uses 'X Other Credits for: $amt' and 'X ATM/DEBIT Deposits: $amt' format. Both should be summed for total deposits.",
  },
  {
    name: "Farmers State Bank",
    aliases: ["Farmers State"],
    identifyPattern: /\bFarmers\s+State\s+Bank\b|\bFarmers\s+State\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?|WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Origin Bank",
    aliases: ["Origin"],
    identifyPattern: /\bOrigin\s+Bank\b|\bOrigin\s+Bancorp\b/i,
    dateFormats: [/(\d{2}\/\d{2}\/\d{2})/, /(\d{2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS|DEPOSITS?\s+TO\s+ACCOUNT/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+WITHDRAWALS?|OTHER\s+DEBITS?|WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
        /Deposits?\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
    notes: "Origin Bank statements may show account as '00371-4650 (0)' format. Last 4 of the numeric portion before parentheses is the account ID.",
  },
  {
    name: "BOM",
    aliases: ["Bank of Marksville", "BOM Bank"],
    identifyPattern: /\bBOM\b|\bBank\s+of\s+Marksville\b/i,
    dateFormats: [/(\d{2}\/\d{2}\/\d{2})/, /(\d{2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS|AUTOMATIC\s+DEPOSITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|AUTOMATIC\s+WITHDRAWALS?|DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Deposits?\s+(\d+)\s+([\d,]+\.\d{2})\+?/i,
        /Automatic\s+Deposits?\s+(\d+)\s+([\d,]+\.\d{2})/i,
        /Total\s+Deposits?\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
    notes: "BOM SMALL BUSINESS CHECKING SUMMARY uses Category/Number/Amount columns with multi-category deposits (Deposits + Automatic Deposits must be COMBINED for total). Amounts may have + or - suffix (e.g. '30,344.00+'). OCR may render summary as markdown table with pipe separators (| Deposits | 9 | 30,344.00+ |) — pipe characters must be stripped before regex matching.",
  },
  {
    name: "44 North Credit Union",
    aliases: ["44 North", "44North"],
    identifyPattern: /\b44\s*North\b/i,
    dateFormats: [/(\d{2}\/\d{2}\/\d{2})/, /(\d{2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /CASH\s+PRO\s+CHECKING\s+[\d.,-]+\s+([\d,]+\.\d{2})/i,
        /Total\s+Deposits?\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
    notes: "44 North Credit Union ACCOUNT SUMMARY has Type/Starting Balance/Total Deposits columns with multiple accounts (REGULAR SAVINGS $0 deposits + CASH PRO CHECKING with actual deposits). Use the 'Total Deposits' value from the CASH PRO CHECKING row ONLY — ignore savings account. Parser may sum all credits including non-deposit items; AI reading the 'Total Deposits' column is correct.",
  },
  {
    name: "Central Pacific Bank",
    aliases: ["Central Pacific", "CPB"],
    identifyPattern: /\bCentral\s+Pacific\s+Bank\b|\bCentral\s+Pacific\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{2}\/\d{2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS|OTHER\s+CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|OTHER\s+DEBITS?|OTHER\s+WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
    notes: "Central Pacific Bank 'Business Exceptional Checking' statements use ACCOUNT SUMMARY with separate '+ N Deposits' and '+ N Other Credits' lines. Both must be COMBINED for total deposits (e.g., 7 Deposits $64,240.55 + 12 Other Credits $60,487.17 = $124,727.72). Uses +/- prefixes and count before category label.",
  },
  {
    name: "Comerica",
    aliases: ["Comerica Bank"],
    identifyPattern: /\bComerica\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "First National Bank",
    aliases: ["First National", "FNB"],
    identifyPattern: /\bFirst\s+National\s+Bank\b|\bFNB\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Pinnacle Bank",
    aliases: ["Pinnacle Financial"],
    identifyPattern: /\bPinnacle\s+Bank\b|\bPinnacle\s+Financial\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Centennial Bank",
    aliases: ["Centennial"],
    identifyPattern: /\bCentennial\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Woodforest",
    aliases: ["Woodforest National Bank"],
    identifyPattern: /\bWoodforest\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Glacier Bank",
    aliases: ["Glacier"],
    identifyPattern: /\bGlacier\s+Bank\b|\bGlacier\s+Bancorp\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Associated Bank",
    aliases: ["Associated"],
    identifyPattern: /\bAssociated\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "BBVA",
    aliases: ["BBVA USA", "Compass Bank"],
    identifyPattern: /\bBBVA\b|\bCompass\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Zions Bank",
    aliases: ["Zions Bancorporation"],
    identifyPattern: /\bZions\s+Bank\b|\bZions\s+Bancorporation\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "The Community Bank",
    aliases: ["Community Bank"],
    identifyPattern: /\bCommunity\s+Bank\b|\bThe\s+Community\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?|WITHDRAWALS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Citibank",
    aliases: ["Citi", "Citigroup"],
    identifyPattern: /\bCitibank\b|\bCiti\s+Business\b|\bCitigroup\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS|CREDITS?\s+AND\s+DEPOSITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?|WITHDRAWALS?\s+AND\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
        /Total\s+Credits?\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "First Horizon",
    aliases: ["First Horizon Bank", "First Tennessee"],
    identifyPattern: /\bFirst\s+Horizon\b|\bFirst\s+Tennessee\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Cadence Bank",
    aliases: ["Cadence", "BancorpSouth"],
    identifyPattern: /\bCadence\s+Bank\b|\bBancorpSouth\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Bank OZK",
    aliases: ["OZK", "Bank of the Ozarks"],
    identifyPattern: /\bBank\s+OZK\b|\bBank\s+of\s+the\s+Ozarks\b|\bOZK\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Umpqua Bank",
    aliases: ["Umpqua", "Columbia Banking System"],
    identifyPattern: /\bUmpqua\s+Bank\b|\bUmpqua\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Valley National Bank",
    aliases: ["Valley Bank", "Valley National"],
    identifyPattern: /\bValley\s+National\s+Bank\b|\bValley\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Webster Bank",
    aliases: ["Webster"],
    identifyPattern: /\bWebster\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Hancock Whitney",
    aliases: ["Hancock Whitney Bank", "Whitney Bank"],
    identifyPattern: /\bHancock\s+Whitney\b|\bWhitney\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Prosperity Bank",
    aliases: ["Prosperity Bankshares"],
    identifyPattern: /\bProsperity\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Ameris Bank",
    aliases: ["Ameris"],
    identifyPattern: /\bAmeris\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "East West Bank",
    aliases: ["East West"],
    identifyPattern: /\bEast\s+West\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Cathay Bank",
    aliases: ["Cathay General Bancorp"],
    identifyPattern: /\bCathay\s+Bank\b|\bCathay\s+General\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Banner Bank",
    aliases: ["Banner Financial"],
    identifyPattern: /\bBanner\s+Bank\b|\bBanner\s+Financial\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "First Citizens Bank",
    aliases: ["First Citizens BancShares"],
    identifyPattern: /\bFirst\s+Citizens\s+Bank\b|\bFirst\s+Citizens\s+BancShares\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "South State Bank",
    aliases: ["South State", "CenterState"],
    identifyPattern: /\bSouth\s+State\s+Bank\b|\bCenterState\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "United Community Bank",
    aliases: ["United Community"],
    identifyPattern: /\bUnited\s+Community\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "CrossFirst Bank",
    aliases: ["CrossFirst"],
    identifyPattern: /\bCrossFirst\s+Bank\b|\bCrossFirst\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Renasant Bank",
    aliases: ["Renasant"],
    identifyPattern: /\bRenasant\s+Bank\b|\bRenasant\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Pacific Premier Bank",
    aliases: ["Pacific Premier"],
    identifyPattern: /\bPacific\s+Premier\s+Bank\b|\bPacific\s+Premier\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Washington Federal",
    aliases: ["WaFd Bank"],
    identifyPattern: /\bWashington\s+Federal\b|\bWaFd\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Independent Bank",
    aliases: ["Independent Financial"],
    identifyPattern: /\bIndependent\s+Bank\b|\bIndependent\s+Financial\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "NBT Bank",
    aliases: ["NBT Bancorp"],
    identifyPattern: /\bNBT\s+Bank\b|\bNBT\s+Bancorp\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "HSBC",
    aliases: ["HSBC Bank USA"],
    identifyPattern: /\bHSBC\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS|CREDITS?\s+AND\s+DEPOSITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i,
        /Total\s+Credits?\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Seacoast Bank",
    aliases: ["Seacoast Banking", "Seacoast National Bank"],
    identifyPattern: /\bSeacoast\s+Bank\b|\bSeacoast\s+Banking\b|\bSeacoast\s+National\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Triumph Bank",
    aliases: ["Triumph Financial", "TBK Bank"],
    identifyPattern: /\bTriumph\s+Bank\b|\bTriumph\s+Financial\b|\bTBK\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Veritex Community Credit Union",
    aliases: ["Veritex"],
    identifyPattern: /\bVeritex\s+Community\b|\bVeritex\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Texas Capital Bank",
    aliases: ["Texas Capital"],
    identifyPattern: /\bTexas\s+Capital\s+Bank\b|\bTexas\s+Capital\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Popular Bank",
    aliases: ["Popular Inc", "Banco Popular"],
    identifyPattern: /\bPopular\s+Bank\b|\bBanco\s+Popular\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "EagleBank",
    aliases: ["Eagle Bancorp"],
    identifyPattern: /\bEagleBank\b|\bEagle\s+Bancorp\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "ServisFirst Bank",
    aliases: ["ServisFirst"],
    identifyPattern: /\bServisFirst\s+Bank\b|\bServisFirst\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "TowneBank",
    aliases: ["Towne Bank"],
    identifyPattern: /\bTowneBank\b|\bTowne\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Sandy Spring Bank",
    aliases: ["Sandy Spring"],
    identifyPattern: /\bSandy\s+Spring\s+Bank\b|\bSandy\s+Spring\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Heartland Credit Union",
    aliases: ["Heartland Bank"],
    identifyPattern: /\bHeartland\s+(?:Credit\s+Union|Bank)\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Navy Federal Credit Union",
    aliases: ["Navy Federal", "NFCU"],
    identifyPattern: /\bNavy\s*Federal\b|\bNFCU\b|navyfederal\.org/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /Total\s+Deposits?\s*:?\s*\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
    useDetailedDepositTotal: true,
    notes: "Navy Federal has a 'Summary of your deposit accounts' table with multiple sub-accounts (Business Checking, Membership Savings, etc.) and a 'Totals' row. Use the 'Totals' row Deposits/Credits column which sums all accounts. If AI picks only one sub-account amount, correct to the Totals row value.",
  },
  {
    name: "Mechanics Bank",
    aliases: ["Mechanics"],
    identifyPattern: /\bMechanics\s+Bank\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Atlantic Union Bank",
    aliases: ["Atlantic Union"],
    identifyPattern: /\bAtlantic\s+Union\s+Bank\b|\bAtlantic\s+Union\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Glacier Hills Credit Union",
    aliases: ["Glacier Hills"],
    identifyPattern: /\bGlacier\s+Hills\s+Credit\s+Union\b|\bGlacier\s+Hills\b/i,
    dateFormats: [/(\d{2}\/\d{2})/, /(\d{1,2}\/\d{1,2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS/i,
      withdrawals: /(?:CHECKS?\s+PAID|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [/Total\s+Deposits?\s+(?:and\s+(?:Other\s+)?Credits?\s*)?\$?([\d,]+\.\d{2})/i],
      totalWithdrawals: [],
    },
    transactionLinePatterns: [/^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/],
    columnLayout: "single",
  },
  {
    name: "Community 1st Bank",
    aliases: ["Community 1st", "Community First Bank"],
    identifyPattern: /\bCommunity\s+1st\s+Bank\b|\bCommunity\s+1st\b/i,
    dateFormats: [/(\d{2}\/\d{2}\/\d{2})/, /(\d{2}\/\d{2})/],
    sectionHeaders: {
      deposits: /DEPOSITS?\s+AND\s+(?:OTHER\s+)?CREDITS|CHECKING\s+ACCOUNTS/i,
      withdrawals: /(?:CHECKS?\s+(?:AND\s+)?(?:DEBITS?|PAID)|ELECTRONIC\s+(?:PAYMENTS?|WITHDRAWALS?)|OTHER\s+DEBITS?)/i,
    },
    summaryPatterns: {
      totalDeposits: [
        /(\d+)\s+Deposits?\/Credits?\s+\$?([\d,]+\.\d{2})/i,
        /Total\s+Deposits?\s+\$?([\d,]+\.\d{2})/i,
        /Deposits?\s+and\s+(?:Other\s+)?Credits?\s+\$?([\d,]+\.\d{2})/i,
      ],
      totalWithdrawals: [
        /(\d+)\s+Checks?\/Debits?\s+\$?([\d,]+\.\d{2})/i,
      ],
    },
    transactionLinePatterns: [
      /^(\d{2}\/\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
      /^(\d{2}\/\d{2})\s+(.+?)\s+(\d[\d,]*\.\d{2})\s*$/,
    ],
    columnLayout: "single",
  },
];

export function identifyBank(rawText: string): BankTemplate | null {
  const patternMatches: BankTemplate[] = [];
  for (const template of BANK_TEMPLATES) {
    if (template.identifyPattern.test(rawText)) {
      patternMatches.push(template);
    }
  }

  // If we have pattern matches, we ONLY consider those. 
  const candidatePool = patternMatches.length > 0 ? patternMatches : BANK_TEMPLATES;

  const textLower = rawText.toLowerCase();
  const firstLines = rawText.split("\n").slice(0, 30).join("\n").toLowerCase();
  const urlDomainPattern = /\b(\w+)\.com\b|\b(\w+)\.org\b|\b(\w+)\.net\b/gi;
  const domainNames = new Set<string>();
  for (const m of rawText.matchAll(urlDomainPattern)) {
    domainNames.add((m[1] || m[2] || m[3]).toLowerCase());
  }

  const headerMatch = candidatePool.find(t => {
    const nameMatch = new RegExp(`\\b${t.name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i").test(firstLines);
    if (nameMatch) return true;
    for (const alias of (t.aliases || [])) {
      const aliasMatch = new RegExp(`\\b${alias.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`, "i").test(firstLines);
      if (aliasMatch) return true;
      if (domainNames.has(alias.toLowerCase())) return true;
    }
    return false;
  });
  if (headerMatch) return headerMatch;

  if (patternMatches.length > 0) {
    return patternMatches[0];
  }

  const nameMatch = candidatePool.find(t => textLower.includes(t.name.toLowerCase()));
  if (nameMatch) return nameMatch;
  const aliasMatch = candidatePool.find(t => t.aliases?.some(a => textLower.includes(a.toLowerCase())));
  if (aliasMatch) return aliasMatch;
  
  return null;
}

export function findBankByName(bankName: string): BankTemplate | null {
  if (!bankName) return null;
  const lower = bankName.toLowerCase().trim();
  const exact = BANK_TEMPLATES.find(t => t.name.toLowerCase() === lower);
  if (exact) return exact;
  const aliasMatch = BANK_TEMPLATES.find(t => t.aliases.some(a => a.toLowerCase() === lower));
  if (aliasMatch) return aliasMatch;
  const partialName = BANK_TEMPLATES.find(t => lower.includes(t.name.toLowerCase()) || t.name.toLowerCase().includes(lower));
  if (partialName) return partialName;
  const partialAlias = BANK_TEMPLATES.find(t => t.aliases.some(a => lower.includes(a.toLowerCase()) || a.toLowerCase().includes(lower)));
  if (partialAlias) return partialAlias;
  return null;
}

export function getAllBankNames(): string[] {
  return BANK_TEMPLATES.map(t => t.name);
}

export { BANK_TEMPLATES };
