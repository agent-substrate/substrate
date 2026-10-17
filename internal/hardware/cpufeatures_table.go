// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hardware

// CPU feature tables for the cpu_features compatibility attribute.
//
// `runsc cpu-features` prints the features a gVisor sandbox exposes by name,
// and the control plane needs them as a bitmask it can compare by subset.
// Each table maps a bit to the name gVisor prints for it. A bit is the value
// of the gVisor Feature constant named in the entry's comment: on amd64,
// 32*block + the feature's bit in that block's CPUID register, which the
// hardware fixes; on arm64, the value gVisor declares. A bit therefore never
// moves between gVisor versions, and a bit with no entry is one the hardware
// reserves.
//
// The tables were produced once from gVisor's pkg/cpuid sources at
// gvisor.dev/gvisor@v0.0.0-20261014212059-ddc37c698141: for every constant of
// type Feature in features_amd64.go and features_arm64.go with an entry in
// that file's allFeatures map, the constant's value (evaluated with go/types)
// keyed to the entry's display name. To take features from a newer gVisor,
// add its new constants the same way; existing entries do not change.
//
// TODO: Remove the tables, EncodeCPUFeatures, and DecodeCPUFeatures once
// runsc can print its feature set as this bitmask itself, so atelet records
// the value directly instead of mapping names. The mask format and the
// subset matching stay.
var (
	amd64CPUFeatures = map[int]string{
		0:   "pni",                 // X86FeatureSSE3 (block 0, bit 0)
		1:   "pclmulqdq",           // X86FeaturePCLMULDQ (block 0, bit 1)
		2:   "dtes64",              // X86FeatureDTES64 (block 0, bit 2)
		3:   "monitor",             // X86FeatureMONITOR (block 0, bit 3)
		4:   "ds_cpl",              // X86FeatureDSCPL (block 0, bit 4)
		5:   "vmx",                 // X86FeatureVMX (block 0, bit 5)
		6:   "smx",                 // X86FeatureSMX (block 0, bit 6)
		7:   "est",                 // X86FeatureEST (block 0, bit 7)
		8:   "tm2",                 // X86FeatureTM2 (block 0, bit 8)
		9:   "ssse3",               // X86FeatureSSSE3 (block 0, bit 9)
		10:  "cid",                 // X86FeatureCNXTID (block 0, bit 10)
		11:  "sdbg",                // X86FeatureSDBG (block 0, bit 11)
		12:  "fma",                 // X86FeatureFMA (block 0, bit 12)
		13:  "cx16",                // X86FeatureCX16 (block 0, bit 13)
		14:  "xtpr",                // X86FeatureXTPR (block 0, bit 14)
		15:  "pdcm",                // X86FeaturePDCM (block 0, bit 15)
		17:  "pcid",                // X86FeaturePCID (block 0, bit 17)
		18:  "dca",                 // X86FeatureDCA (block 0, bit 18)
		19:  "sse4_1",              // X86FeatureSSE4_1 (block 0, bit 19)
		20:  "sse4_2",              // X86FeatureSSE4_2 (block 0, bit 20)
		21:  "x2apic",              // X86FeatureX2APIC (block 0, bit 21)
		22:  "movbe",               // X86FeatureMOVBE (block 0, bit 22)
		23:  "popcnt",              // X86FeaturePOPCNT (block 0, bit 23)
		24:  "tsc_deadline_timer",  // X86FeatureTSCD (block 0, bit 24)
		25:  "aes",                 // X86FeatureAES (block 0, bit 25)
		26:  "xsave",               // X86FeatureXSAVE (block 0, bit 26)
		27:  "osxsave",             // X86FeatureOSXSAVE (block 0, bit 27)
		28:  "avx",                 // X86FeatureAVX (block 0, bit 28)
		29:  "f16c",                // X86FeatureF16C (block 0, bit 29)
		30:  "rdrand",              // X86FeatureRDRAND (block 0, bit 30)
		31:  "hypervisor",          // X86FeatureHypervisor (block 0, bit 31)
		32:  "fpu",                 // X86FeatureFPU (block 1, bit 0)
		33:  "vme",                 // X86FeatureVME (block 1, bit 1)
		34:  "de",                  // X86FeatureDE (block 1, bit 2)
		35:  "pse",                 // X86FeaturePSE (block 1, bit 3)
		36:  "tsc",                 // X86FeatureTSC (block 1, bit 4)
		37:  "msr",                 // X86FeatureMSR (block 1, bit 5)
		38:  "pae",                 // X86FeaturePAE (block 1, bit 6)
		39:  "mce",                 // X86FeatureMCE (block 1, bit 7)
		40:  "cx8",                 // X86FeatureCX8 (block 1, bit 8)
		41:  "apic",                // X86FeatureAPIC (block 1, bit 9)
		43:  "sep",                 // X86FeatureSEP (block 1, bit 11)
		44:  "mtrr",                // X86FeatureMTRR (block 1, bit 12)
		45:  "pge",                 // X86FeaturePGE (block 1, bit 13)
		46:  "mca",                 // X86FeatureMCA (block 1, bit 14)
		47:  "cmov",                // X86FeatureCMOV (block 1, bit 15)
		48:  "pat",                 // X86FeaturePAT (block 1, bit 16)
		49:  "pse36",               // X86FeaturePSE36 (block 1, bit 17)
		50:  "pn",                  // X86FeaturePSN (block 1, bit 18)
		51:  "clflush",             // X86FeatureCLFSH (block 1, bit 19)
		53:  "dts",                 // X86FeatureDS (block 1, bit 21)
		54:  "acpi",                // X86FeatureACPI (block 1, bit 22)
		55:  "mmx",                 // X86FeatureMMX (block 1, bit 23)
		56:  "fxsr",                // X86FeatureFXSR (block 1, bit 24)
		57:  "sse",                 // X86FeatureSSE (block 1, bit 25)
		58:  "sse2",                // X86FeatureSSE2 (block 1, bit 26)
		59:  "ss",                  // X86FeatureSS (block 1, bit 27)
		60:  "ht",                  // X86FeatureHTT (block 1, bit 28)
		61:  "tm",                  // X86FeatureTM (block 1, bit 29)
		62:  "ia64",                // X86FeatureIA64 (block 1, bit 30)
		63:  "pbe",                 // X86FeaturePBE (block 1, bit 31)
		64:  "fsgsbase",            // X86FeatureFSGSBase (block 2, bit 0)
		65:  "tsc_adjust",          // X86FeatureTSC_ADJUST (block 2, bit 1)
		67:  "bmi1",                // X86FeatureBMI1 (block 2, bit 3)
		68:  "hle",                 // X86FeatureHLE (block 2, bit 4)
		69:  "avx2",                // X86FeatureAVX2 (block 2, bit 5)
		70:  "fdp_excptn_only",     // X86FeatureFDP_EXCPTN_ONLY (block 2, bit 6)
		71:  "smep",                // X86FeatureSMEP (block 2, bit 7)
		72:  "bmi2",                // X86FeatureBMI2 (block 2, bit 8)
		73:  "erms",                // X86FeatureERMS (block 2, bit 9)
		74:  "invpcid",             // X86FeatureINVPCID (block 2, bit 10)
		75:  "rtm",                 // X86FeatureRTM (block 2, bit 11)
		76:  "cqm",                 // X86FeatureCQM (block 2, bit 12)
		77:  "fpcsds",              // X86FeatureFPCSDS (block 2, bit 13)
		78:  "mpx",                 // X86FeatureMPX (block 2, bit 14)
		79:  "rdt_a",               // X86FeatureRDT (block 2, bit 15)
		80:  "avx512f",             // X86FeatureAVX512F (block 2, bit 16)
		81:  "avx512dq",            // X86FeatureAVX512DQ (block 2, bit 17)
		82:  "rdseed",              // X86FeatureRDSEED (block 2, bit 18)
		83:  "adx",                 // X86FeatureADX (block 2, bit 19)
		84:  "smap",                // X86FeatureSMAP (block 2, bit 20)
		87:  "clfushopt",           // X86FeatureCLFLUSHOPT (block 2, bit 23)
		88:  "clwb",                // X86FeatureCLWB (block 2, bit 24)
		89:  "ipt",                 // X86FeatureIPT (block 2, bit 25)
		90:  "avx512pf",            // X86FeatureAVX512PF (block 2, bit 26)
		91:  "avx512er",            // X86FeatureAVX512ER (block 2, bit 27)
		92:  "avx512cd",            // X86FeatureAVX512CD (block 2, bit 28)
		93:  "sha_ni",              // X86FeatureSHA (block 2, bit 29)
		94:  "avx512bw",            // X86FeatureAVX512BW (block 2, bit 30)
		95:  "avx512vl",            // X86FeatureAVX512VL (block 2, bit 31)
		96:  "prefetchwt1",         // X86FeaturePREFETCHWT1 (block 3, bit 0)
		97:  "avx512vbmi",          // X86FeatureAVX512VBMI (block 3, bit 1)
		98:  "umip",                // X86FeatureUMIP (block 3, bit 2)
		99:  "pku",                 // X86FeaturePKU (block 3, bit 3)
		100: "ospke",               // X86FeatureOSPKE (block 3, bit 4)
		101: "waitpkg",             // X86FeatureWAITPKG (block 3, bit 5)
		102: "avx512_vbmi2",        // X86FeatureAVX512_VBMI2 (block 3, bit 6)
		103: "cet_ss",              // X86FeatureCET_SS (block 3, bit 7)
		104: "gfni",                // X86FeatureGFNI (block 3, bit 8)
		105: "vaes",                // X86FeatureVAES (block 3, bit 9)
		106: "vpclmulqdq",          // X86FeatureVPCLMULQDQ (block 3, bit 10)
		107: "avx512_vnni",         // X86FeatureAVX512_VNNI (block 3, bit 11)
		108: "avx512_bitalg",       // X86FeatureAVX512_BITALG (block 3, bit 12)
		109: "tme",                 // X86FeatureTME (block 3, bit 13)
		110: "avx512_vpopcntdq",    // X86FeatureAVX512_VPOPCNTDQ (block 3, bit 14)
		112: "la57",                // X86FeatureLA57 (block 3, bit 16)
		118: "rdpid",               // X86FeatureRDPID (block 3, bit 22)
		121: "cldemote",            // X86FeatureCLDEMOTE (block 3, bit 25)
		123: "movdiri",             // X86FeatureMOVDIRI (block 3, bit 27)
		124: "movdir64b",           // X86FeatureMOVDIR64B (block 3, bit 28)
		128: "xsaveopt",            // X86FeatureXSAVEOPT (block 4, bit 0)
		129: "xsavec",              // X86FeatureXSAVEC (block 4, bit 1)
		130: "xgetbv1",             // X86FeatureXGETBV1 (block 4, bit 2)
		131: "xsaves",              // X86FeatureXSAVES (block 4, bit 3)
		160: "lahf_lm",             // X86FeatureLAHF64 (block 5, bit 0)
		161: "cmp_legacy",          // X86FeatureCMP_LEGACY (block 5, bit 1)
		162: "svm",                 // X86FeatureSVM (block 5, bit 2)
		163: "extapic",             // X86FeatureEXTAPIC (block 5, bit 3)
		164: "cr8_legacy",          // X86FeatureCR8_LEGACY (block 5, bit 4)
		165: "abm",                 // X86FeatureLZCNT (block 5, bit 5)
		166: "sse4a",               // X86FeatureSSE4A (block 5, bit 6)
		167: "misalignsse",         // X86FeatureMISALIGNSSE (block 5, bit 7)
		168: "3dnowprefetch",       // X86FeaturePREFETCHW (block 5, bit 8)
		169: "osvw",                // X86FeatureOSVW (block 5, bit 9)
		170: "ibs",                 // X86FeatureIBS (block 5, bit 10)
		171: "xop",                 // X86FeatureXOP (block 5, bit 11)
		172: "skinit",              // X86FeatureSKINIT (block 5, bit 12)
		173: "wdt",                 // X86FeatureWDT (block 5, bit 13)
		175: "lwp",                 // X86FeatureLWP (block 5, bit 15)
		176: "fma4",                // X86FeatureFMA4 (block 5, bit 16)
		177: "tce",                 // X86FeatureTCE (block 5, bit 17)
		181: "tbm",                 // X86FeatureTBM (block 5, bit 21)
		182: "topoext",             // X86FeatureTOPOLOGY (block 5, bit 22)
		183: "perfctr_core",        // X86FeaturePERFCTR_CORE (block 5, bit 23)
		184: "perfctr_nb",          // X86FeaturePERFCTR_NB (block 5, bit 24)
		186: "bpext",               // X86FeatureBPEXT (block 5, bit 26)
		187: "ptsc",                // X86FeaturePERFCTR_TSC (block 5, bit 27)
		188: "perfctr_llc",         // X86FeaturePERFCTR_LLC (block 5, bit 28)
		189: "mwaitx",              // X86FeatureMWAITX (block 5, bit 29)
		190: "ad_mask_extn",        // X86FeatureADMSKEXTN (block 5, bit 30)
		203: "syscall",             // X86FeatureSYSCALL (block 6, bit 11)
		212: "nx",                  // X86FeatureNX (block 6, bit 20)
		214: "mmxext",              // X86FeatureMMXEXT (block 6, bit 22)
		217: "fxsr_opt",            // X86FeatureFXSR_OPT (block 6, bit 25)
		218: "pdpe1gb",             // X86FeatureGBPAGES (block 6, bit 26)
		219: "rdtscp",              // X86FeatureRDTSCP (block 6, bit 27)
		221: "lm",                  // X86FeatureLM (block 6, bit 29)
		222: "3dnowext",            // X86Feature3DNOWEXT (block 6, bit 30)
		223: "3dnow",               // X86Feature3DNOW (block 6, bit 31)
		226: "avx512_4vnniw",       // X86FeatureAVX512_4VNNIW (block 7, bit 2)
		227: "avx512_4fmaps",       // X86FeatureAVX512_4FMAPS (block 7, bit 3)
		228: "fsrm",                // X86FeatureFSRM (block 7, bit 4)
		232: "avx512_vp2intersect", // X86FeatureAVX512_VP2INTERSECT (block 7, bit 8)
		233: "srbds_ctrl",          // X86FeatureSRBDS_CTRL (block 7, bit 9)
		234: "md_clear",            // X86FeatureMD_CLEAR (block 7, bit 10)
		235: "rtm_always_abort",    // X86FeatureRTM_ALWAYS_ABORT (block 7, bit 11)
		237: "tsx_force_abort",     // X86FeatureTSX_FORCE_ABORT (block 7, bit 13)
		238: "serialize",           // X86FeatureSERIALIZE (block 7, bit 14)
		239: "hybrid_cpu",          // X86FeatureHYBRID_CPU (block 7, bit 15)
		240: "tsxldtrk",            // X86FeatureTSXLDTRK (block 7, bit 16)
		242: "pconfig",             // X86FeaturePCONFIG (block 7, bit 18)
		243: "arch_lbr",            // X86FeatureARCH_LBR (block 7, bit 19)
		244: "ibt",                 // X86FeatureIBT (block 7, bit 20)
		246: "amx_bf16",            // X86FeatureAMX_BF16 (block 7, bit 22)
		247: "avx512_fp16",         // X86FeatureAVX512_FP16 (block 7, bit 23)
		248: "amx_tile",            // X86FeatureAMX_TILE (block 7, bit 24)
		249: "amx_int8",            // X86FeatureAMX_INT8 (block 7, bit 25)
		250: "spec_ctrl",           // X86FeatureSPEC_CTRL (block 7, bit 26)
		251: "intel_stibp",         // X86FeatureINTEL_STIBP (block 7, bit 27)
		252: "flush_l1d",           // X86FeatureFLUSH_L1D (block 7, bit 28)
		253: "arch_capabilities",   // X86FeatureARCH_CAPABILITIES (block 7, bit 29)
		254: "core_capabilities",   // X86FeatureCORE_CAPABILITIES (block 7, bit 30)
		255: "spec_ctrl_ssbd",      // X86FeatureSPEC_CTRL_SSBD (block 7, bit 31)
		260: "avx_vnni",            // X86FeatureAVX_VNNI (block 8, bit 4)
		261: "avx512_bf16",         // X86FeatureAVX512_BF16 (block 8, bit 5)
	}

	arm64CPUFeatures = map[int]string{
		0:  "fp",       // ARM64FeatureFP
		1:  "asimd",    // ARM64FeatureASIMD
		2:  "evtstrm",  // ARM64FeatureEVTSTRM
		3:  "aes",      // ARM64FeatureAES
		4:  "pmull",    // ARM64FeaturePMULL
		5:  "sha1",     // ARM64FeatureSHA1
		6:  "sha2",     // ARM64FeatureSHA2
		7:  "crc32",    // ARM64FeatureCRC32
		8:  "atomics",  // ARM64FeatureATOMICS
		9:  "fphp",     // ARM64FeatureFPHP
		10: "asimdhp",  // ARM64FeatureASIMDHP
		11: "cpuid",    // ARM64FeatureCPUID
		12: "asimdrdm", // ARM64FeatureASIMDRDM
		13: "jscvt",    // ARM64FeatureJSCVT
		14: "fcma",     // ARM64FeatureFCMA
		15: "lrcpc",    // ARM64FeatureLRCPC
		16: "dcpop",    // ARM64FeatureDCPOP
		17: "sha3",     // ARM64FeatureSHA3
		18: "sm3",      // ARM64FeatureSM3
		19: "sm4",      // ARM64FeatureSM4
		20: "asimddp",  // ARM64FeatureASIMDDP
		21: "sha512",   // ARM64FeatureSHA512
		22: "sve",      // ARM64FeatureSVE
		23: "asimdfhm", // ARM64FeatureASIMDFHM
	}
)
