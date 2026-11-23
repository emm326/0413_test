using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using SolidWorks.Interop.sldworks;
using SolidWorks.Interop.swconst;

internal static partial class BuildNodeEnclosure
{
    private const string PrintableFile = "Dual_Layer_Printable_84x66.SLDASM";
    private const double PrintPowerStand = 3.2;
    private static readonly double[,] UsbClipHoles = { { -29, -22.2 }, { -29, 9.2 } };
    private static readonly string[] PrintableParts = { "DP_Base", "DP_Upper_USB", "DP_Upper_Bolt", "DP_Edge_Clip", "DP_Hanging_Plate", "DP_Lid" };

    private static int BuildPrintable(string directory, bool assembleOnly = false)
    {
        output = Path.GetFullPath(directory);
        Directory.CreateDirectory(output);
        Require(assembleOnly || !Directory.EnumerateFileSystemEntries(output).Any(), "Output directory is not empty.");
        Require(!File.Exists(Path.Combine(output, PrintableFile)), "Printable assembly already exists.");
        sw = (ISldWorks)Marshal.GetActiveObject("SldWorks.Application");
        var parts = assembleOnly ? PrintableParts.Select(LoadPart).ToArray() : new[]
        {
            MeasuredBase("DP_Base"), PrintUsbEnd(), PrintBoltEnd(), DualClip("DP_Edge_Clip"),
            PrintHangingPlate(), MeasuredLid("DP_Lid")
        };
        var reference = assembleOnly ? LoadPart("DP_Layout_References") : MeasuredReferences(true);
        ExportPrintableParts(parts);
        var doc = NewDoc(true);
        var asm = (AssemblyDoc)doc;
        doc.IGetActiveConfiguration().Name = DualClosed;
        PlaceDual(doc, parts[0], XY, 0, 0, 0);
        PlaceDual(doc, parts[1], XY, 0, 0, ShelfZ);
        PlaceDual(doc, parts[2], XY, 0, 0, ShelfZ);
        for (int i = 0; i < UsbClipHoles.GetLength(0); i++)
            PlaceDual(doc, parts[3], i == 0 ? XY : HalfTurn, UsbClipHoles[i, 0], UsbClipHoles[i, 1], ShelfZ + ShelfT);
        PlaceDual(doc, parts[4], Vertical, 0, PowerY, PowerZ);
        PlaceDual(doc, parts[5], XY, 0, 0, DualRim);
        PlaceDual(doc, reference, XY, 0, 0, 0);
        CheckDualState(doc, 8, true);
        AddDualConfig(doc, DualLayout, "Lid removed; shows the printed parts and nominal electronic envelopes");
        ActivateConfiguration(doc, DualClosed);
        AddDualConfig(doc, DualService, "Two upper end supports, clips and lid removed for lower access");
        foreach (string name in new[] { "DP_Lid", "DP_Upper_USB", "DP_Upper_Bolt", "DP_Edge_Clip", "DP_Layout_References" })
            foreach (var component in PartComponents(asm, name)) SetDualSuppression(doc, component);
        ActivateConfiguration(doc, DualClosed);
        SetDualSuppression(doc, PartComponents(asm, "DP_Layout_References").Single());
        ActivateConfiguration(doc, DualLayout);
        SetDualSuppression(doc, PartComponents(asm, "DP_Lid").Single());
        CheckPrintable(doc);
        ActivateConfiguration(doc, DualLayout);
        View(doc);
        Save(doc, PrintableFile);
        sw.CloseDoc(doc.GetTitle());
        sw.CloseDoc(reference.GetTitle());
        foreach (var part in parts) sw.CloseDoc(part.GetTitle());
        return VerifyPrintable(output);
    }

    private static ModelDoc2 PrintUsbEnd()
    {
        var doc = NewDoc(false);
        Add(doc, "USB end crossbar with open connector relief", 0, ShelfT, s =>
        {
            Polygon(s, new double[,] { { -38.8, -21.5 }, { -34.7, -24 }, { -25.5, -24 }, { -25.5, -13 },
                { -35.1, -13 }, { -35.1, 0 }, { -25.5, 0 }, { -25.5, 10.8 }, { -38.8, 10.8 } });
            Circles(s, new double[,] { { -36.8, -17.6 }, { -36.8, 2.2 } }, 1.15);
            Circles(s, UsbClipHoles, 1.15);
        });
        Add(doc, "Two PCB rests beside Type-C, no support under connector", ShelfT, BoardRest, s =>
        {
            Rect(s, -32, -19.8, -26.3, -15.6);
            Rect(s, -32, 2, -26.3, 6.8);
        });
        VerifyPart(doc, 13.3, 34.8, ShelfT + BoardRest);
        return SavePart(doc, "DP_Upper_USB", new[] { .23, .51, .52 });
    }

    private static void PrintBoltSlots(SketchManager s)
    {
        foreach (double y in new[] { -16.5d, 3.5d }) Slot(s, 28.9, y, 5.8, 2.8);
    }

    private static ModelDoc2 PrintBoltEnd()
    {
        var doc = NewDoc(false);
        Add(doc, "Opposite end crossbar with two adjustable through slots", 0, ShelfT, s =>
        {
            Polygon(s, new double[,] { { 25, -24 }, { 34.7, -24 }, { 38.8, -21.5 },
                { 38.8, 10.8 }, { 25, 10.8 } });
            Circles(s, new double[,] { { 36.8, -17.6 }, { 36.8, 2.2 } }, 1.15);
            PrintBoltSlots(s);
        });
        Add(doc, "Two raised PCB bolt pads", ShelfT, BoardRest, s =>
        {
            Rect(s, 25, -19.7, 32.8, -13.3);
            Rect(s, 25, .3, 32.8, 6.7);
            PrintBoltSlots(s);
        });
        VerifyPart(doc, 13.8, 34.8, ShelfT + BoardRest);
        return SavePart(doc, "DP_Upper_Bolt", new[] { .23, .51, .52 });
    }

    private static ModelDoc2 PrintHangingPlate()
    {
        var doc = NewDoc(false);
        double[,] centers = { { -25, -8.5 }, { -25, 8.5 }, { 25, -8.5 }, { 25, 8.5 } };
        Add(doc, "Flat back of separately printed slide-in plate", 0, 1, s => Outline(s, 32, 11.4, .6));
        Add(doc, "Plate rim with blind D2.05 pilot bottoms", 1, 1.2, s =>
        {
            Outline(s, 32, 11.4, .6);
            Circles(s, centers, 1.025);
        });
        Add(doc, "Raised power board supports - no nuts behind slide", 2.2, PrintPowerStand, s =>
        {
            Circles(s, centers, 2.65);
            Circles(s, centers, 1.025);
        });
        VerifyPart(doc, 64, 22.8, 2.2 + PrintPowerStand);
        return SavePart(doc, "DP_Hanging_Plate", new[] { .26, .46, .56 });
    }

    private static void CheckPrintable(ModelDoc2 doc)
    {
        var asm = (AssemblyDoc)doc;
        ActivateConfiguration(doc, DualClosed);
        CheckDualState(doc, 7, true);
        var lid = PartComponents(asm, "DP_Lid").Single();
        Near(1000 * ((double[])lid.Transform2.ArrayData)[11] + DualLidT, 60, "Printed model height");
        ActivateConfiguration(doc, DualLayout);
        CheckDualState(doc, 7, true);
        ActivateConfiguration(doc, DualService);
        CheckDualState(doc, 2, true);
    }

    private static int VerifyPrintable(string directory)
    {
        output = Path.GetFullPath(directory);
        string path = Path.Combine(output, PrintableFile);
        Require(File.Exists(path), "Missing printable assembly.");
        sw = (ISldWorks)Marshal.GetActiveObject("SldWorks.Application");
        var previous = (ModelDoc2)sw.GetOpenDocumentByName(path);
        if (previous != null)
        {
            Require(!previous.GetSaveFlag(), "Save the current printable assembly before rechecking.");
            sw.CloseDoc(previous.GetTitle());
        }
        int err = 0, warn = 0;
        var doc = sw.OpenDoc6(path, (int)swDocumentTypes_e.swDocASSEMBLY, (int)swOpenDocOptions_e.swOpenDocOptions_Silent,
            DualLayout, ref err, ref warn);
        Require(doc != null && err == 0 && (warn & ~(int)swFileLoadWarning_e.swFileLoadWarning_NeedsRegen) == 0,
            "Reopen printable assembly: " + err + "/" + warn);
        foreach (var c in ((object[])((AssemblyDoc)doc).GetComponents(true)).Cast<Component2>())
        {
            string file = c.GetPathName();
            Require(string.Equals(Path.GetDirectoryName(file), output, StringComparison.OrdinalIgnoreCase) && File.Exists(file),
                "Missing or nonlocal component " + file);
        }
        CheckPrintable(doc);
        ActivateConfiguration(doc, DualLayout);
        View(doc);
        Require(doc.Save3((int)swSaveAsOptions_e.swSaveAsOptions_Silent, ref err, ref warn) && err == 0 && warn == 0,
            "Save verified assembly: " + err + "/" + warn);
        foreach (var name in PrintableParts)
        {
            var part = LoadPart(name);
            CheckBinaryStl(Path.Combine(output, "STL", name + ".stl"), part);
        }
        Console.WriteLine("Verified and open: " + path);
        return 0;
    }

    private static void ExportPrintableParts(ModelDoc2[] parts)
    {
        string folder = Path.Combine(output, "STL");
        Directory.CreateDirectory(folder);
        int units = (int)swUserPreferenceIntegerValue_e.swExportStlUnits;
        int quality = (int)swUserPreferenceIntegerValue_e.swSTLQuality;
        var ints = new[] { units, quality };
        var originalInts = ints.Select(i => sw.GetUserPreferenceIntegerValue(i)).ToArray();
        int[] toggles = { (int)swUserPreferenceToggle_e.swSTLBinaryFormat, (int)swUserPreferenceToggle_e.swSTLShowInfoOnSave,
            (int)swUserPreferenceToggle_e.swSTLDontTranslateToPositive, (int)swUserPreferenceToggle_e.swSTLPreview };
        var originalToggles = toggles.Select(i => sw.GetUserPreferenceToggle(i)).ToArray();
        try
        {
            Require(sw.SetUserPreferenceIntegerValue(units, (int)swLengthUnit_e.swMM), "Set STL units to millimeters.");
            Require(sw.SetUserPreferenceIntegerValue(quality, (int)swSTLQuality_e.swSTLQuality_Fine), "Set fine STL tessellation.");
            for (int i = 0; i < toggles.Length; i++) sw.SetUserPreferenceToggle(toggles[i], i == 0 || i == 2);
            foreach (var part in parts)
            {
                int error = 0, warning = 0;
                Require(sw.ActivateDoc3(part.GetTitle(), false, (int)swRebuildOnActivation_e.swDontRebuildActiveDoc, ref error) != null && error == 0,
                    "Activate part for STL export: " + part.GetTitle());
                string path = Path.Combine(folder, Path.GetFileNameWithoutExtension(part.GetPathName()) + ".stl");
                if (!File.Exists(path))
                    Require(part.Extension.SaveAs3(path, 0, (int)swSaveAsOptions_e.swSaveAsOptions_Silent,
                        null, null, ref error, ref warning) && error == 0 && warning == 0, "STL export: " + path + " " + error + "/" + warning);
                CheckBinaryStl(path, part);
            }
        }
        finally
        {
            for (int i = 0; i < ints.Length; i++) sw.SetUserPreferenceIntegerValue(ints[i], originalInts[i]);
            for (int i = 0; i < toggles.Length; i++) sw.SetUserPreferenceToggle(toggles[i], originalToggles[i]);
        }
    }

    private static void CheckBinaryStl(string file, ModelDoc2 part)
    {
        string name = Path.GetFileName(file);
        var meshMin = new[] { double.PositiveInfinity, double.PositiveInfinity, double.PositiveInfinity };
        var meshMax = new[] { double.NegativeInfinity, double.NegativeInfinity, double.NegativeInfinity };
        var edges = new Dictionary<long, int>();
        var vertexIds = new Dictionary<Tuple<int, int, int>, int>();
        var connected = new List<int>();
        double orientedVolume = 0;
        uint triangles;
        using (var stream = File.OpenRead(file))
        using (var input = new BinaryReader(stream))
        {
            input.ReadBytes(80);
            triangles = input.ReadUInt32();
            Require(triangles > 3 && stream.Length == 84L + 50L * triangles, name + " is not a full binary STL.");
            for (uint i = 0; i < triangles; i++)
            {
                input.ReadBytes(12);
                var v = new double[3, 3];
                var ids = new int[3];
                for (int j = 0; j < 3; j++)
                {
                    for (int k = 0; k < 3; k++)
                    {
                        double c = input.ReadSingle();
                        Require(!double.IsNaN(c) && !double.IsInfinity(c), name + " non-finite vertex.");
                        v[j, k] = c;
                        meshMin[k] = Math.Min(meshMin[k], c);
                        meshMax[k] = Math.Max(meshMax[k], c);
                    }
                    var key = Tuple.Create(Quantize(v[j, 0]), Quantize(v[j, 1]), Quantize(v[j, 2]));
                    if (!vertexIds.TryGetValue(key, out ids[j]))
                    {
                        ids[j] = vertexIds.Count;
                        vertexIds.Add(key, ids[j]);
                        connected.Add(ids[j]);
                    }
                }
                for (int j = 0; j < 3; j++)
                {
                    int a = ids[j], b = ids[(j + 1) % 3];
                    Require(a != b, name + " degenerate facet edge.");
                    long key = ((long)Math.Min(a, b) << 32) | (uint)Math.Max(a, b);
                    int orientations;
                    edges.TryGetValue(key, out orientations);
                    edges[key] = orientations + (a < b ? 1 : 65536);
                    connected[VertexRoot(connected, a)] = VertexRoot(connected, b);
                }
                orientedVolume += (v[0, 0] * (v[1, 1] * v[2, 2] - v[1, 2] * v[2, 1]) +
                    v[0, 1] * (v[1, 2] * v[2, 0] - v[1, 0] * v[2, 2]) +
                    v[0, 2] * (v[1, 0] * v[2, 1] - v[1, 1] * v[2, 0])) / 6;
                input.ReadUInt16();
            }
        }
        Require(edges.Values.All(count => count == 65537), name + " has an open, misoriented or nonmanifold edge.");
        var root = VertexRoot(connected, 0);
        Require(Enumerable.Range(1, connected.Count - 1).All(i => VertexRoot(connected, i) == root), name + " contains disconnected shells.");
        double[] box = (double[])Solids(part).Single().GetBodyBox();
        for (int i = 0; i < 3; i++)
            Require(Math.Abs(meshMax[i] - meshMin[i] - (box[i + 3] - box[i]) * 1000) < .06, name + " scale differs from the native CAD solid.");
        Near(meshMin[2], 0, name + " flat print plane");
        double solidVolume = VolumeMm3(part);
        Require(Math.Abs(orientedVolume - solidVolume) < .015 * solidVolume, name + " volume differs from CAD.");
        Console.WriteLine("STL {0}: {1} facets, single closed oriented shell, millimeter scale checked.", name, triangles);
    }

    private static int Quantize(double v) { return (int)Math.Round(v * 10000); }

    private static int VertexRoot(List<int> links, int i)
    {
        while (links[i] != i)
        {
            links[i] = links[links[i]];
            i = links[i];
        }
        return i;
    }
}
