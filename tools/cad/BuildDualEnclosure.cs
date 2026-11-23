using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using SolidWorks.Interop.sldworks;
using SolidWorks.Interop.swconst;

internal static partial class BuildNodeEnclosure
{
    private const double DualFloor = 2.8, DualRim = 57.4, DualLidT = 2.6;
    private const double ShelfZ = 28.4, ShelfT = 2.6, BoardRest = 2, BoardT = 1.6;
    private const double PlugSeat = 2, Dupont = 16;
    private const string DualFile = "Dual_Layer_Sensor_Enclosure.SLDASM";
    private const string DualClosed = "Closed", DualLayout = "Board_Layout", DualService = "Lower_Access";
    private static readonly double[,] DualLidPosts = { { -39, -28.5 }, { 39, -28.5 }, { -39, 28.5 }, { 39, 28.5 } };
    private static readonly double[,] DualCaseHoles = { { -39, -8 }, { 39, -8 }, { -39, 20 }, { 39, 20 } };
    private static readonly double[,] ShelfPosts = { { -38.8, -18.5 }, { 38.8, -18.5 }, { -38.8, 2.5 }, { 38.8, 2.5 } };
    private static readonly double[,] ClipHoles = { { -23, -22.7 }, { 25, -22.7 }, { -23, 8.7 }, { 25, 8.7 } };
    private static readonly double[,] SensorPosts = { { 7, 28 }, { 26, 28 } };
    private static readonly double[,] GnssPosts = { { -29.25, 8.3 }, { -29.25, 28.8 } };
    private static readonly double[] XY = { 1, 0, 0, 0, 1, 0, 0, 0, 1 };
    private static readonly double[] HalfTurn = { -1, 0, 0, 0, -1, 0, 0, 0, 1 };
    private static readonly double[] Vertical = { 1, 0, 0, 0, 0, 1, 0, -1, 0 };

    private static int BuildDual(string directory, bool assembleOnly = false)
    {
        output = Path.GetFullPath(directory);
        Directory.CreateDirectory(output);
        Require(assembleOnly || !Directory.EnumerateFileSystemEntries(output).Any(), "Output directory is not empty.");
        Require(!File.Exists(Path.Combine(output, DualFile)), "Dual assembly already exists.");
        sw = (ISldWorks)Marshal.GetActiveObject("SldWorks.Application");
        Console.WriteLine("SolidWorks " + sw.RevisionNumber());
        var parts = assembleOnly ? new[] { LoadPart("DL_Base"), LoadPart("DL_Upper_Shelf"), LoadPart("DL_Edge_Clip"),
            LoadPart("DL_Power_Plate"), LoadPart("DL_Lid"), LoadPart("DL_Layout_References") }
            : new[] { DualBase(), DualShelf(), DualClip(), DualPowerPlate(), DualLid(), DualReferences() };
        return AssembleDual(parts, ClipHoles, 30.3, 23.2, false);
    }

    private static int AssembleDual(ModelDoc2[] parts, double[,] clips, double powerY, double powerZ, bool measured)
    {
        string prefix = measured ? "DM_" : "DL_";
        string file = measured ? MeasuredFile : DualFile;
        var doc = NewDoc(true);
        var assembly = (AssemblyDoc)doc;
        doc.IGetActiveConfiguration().Name = DualClosed;
        PlaceDual(doc, parts[0], XY, 0, 0, 0);
        PlaceDual(doc, parts[1], XY, 0, 0, ShelfZ);
        for (int i = 0; i < clips.GetLength(0); i++)
            PlaceDual(doc, parts[2], i < 2 ? XY : HalfTurn, clips[i, 0], clips[i, 1], ShelfZ + ShelfT);
        PlaceDual(doc, parts[3], Vertical, 0, powerY, powerZ);
        PlaceDual(doc, parts[4], XY, 0, 0, DualRim);
        var reference = PlaceDual(doc, parts[5], XY, 0, 0, 0);
        string referenceName = reference.Name2;
        // Test the closed model including nominal component bodies before switching them off.
        CheckDualState(doc, 9, true);
        AddDualConfig(doc, DualLayout, "Lid removed; shows the nominal electronic envelopes");
        ActivateConfiguration(doc, DualClosed);
        AddDualConfig(doc, DualService, "Lid, upper shelf, clips and electronic envelopes removed");
        SetDualSuppression(doc, PartComponents(assembly, prefix + "Lid").Single());
        foreach (var item in PartComponents(assembly, prefix + "Upper_Shelf").Concat(PartComponents(assembly, prefix + "Edge_Clip")))
            SetDualSuppression(doc, item);
        SetDualSuppression(doc, assembly.GetComponentByName(referenceName));
        ActivateConfiguration(doc, DualClosed);
        SetDualSuppression(doc, assembly.GetComponentByName(referenceName));
        ActivateConfiguration(doc, DualLayout);
        SetDualSuppression(doc, PartComponents(assembly, prefix + "Lid").Single());
        VerifyDual(doc, prefix);
        ActivateConfiguration(doc, DualLayout);
        View(doc);
        Save(doc, file);
        sw.CloseDoc(doc.GetTitle());
        foreach (var part in parts) sw.CloseDoc(part.GetTitle());
        return VerifySavedDual(output, measured);
    }

    private static int VerifySavedDual(string directory, bool measured = false)
    {
        output = Path.GetFullPath(directory);
        string path = Path.Combine(output, measured ? MeasuredFile : DualFile);
        Require(File.Exists(path), "Missing assembly: " + path);
        sw = (ISldWorks)Marshal.GetActiveObject("SldWorks.Application");
        var previous = (ModelDoc2)sw.GetOpenDocumentByName(path);
        if (previous != null)
        {
            Require(!previous.GetSaveFlag(), "Save changes to the open dual assembly before verification.");
            sw.CloseDoc(previous.GetTitle());
        }
        int err = 0, warn = 0;
        var reopened = sw.OpenDoc6(path, (int)swDocumentTypes_e.swDocASSEMBLY,
            (int)swOpenDocOptions_e.swOpenDocOptions_Silent, DualLayout, ref err, ref warn);
        int rebuildWarning = (int)swFileLoadWarning_e.swFileLoadWarning_NeedsRegen;
        Require(reopened != null && err == 0 && (warn & ~rebuildWarning) == 0, "Reopen dual assembly: " + err + "/" + warn);
        if (warn == rebuildWarning) Console.WriteLine("SolidWorks requested a rebuild on reopen.");
        var components = (object[])((AssemblyDoc)reopened).GetComponents(true);
        foreach (var component in components.Cast<Component2>())
        {
            string componentPath = component.GetPathName();
            Require(string.Equals(Path.GetDirectoryName(componentPath), output, StringComparison.OrdinalIgnoreCase) &&
                File.Exists(componentPath), "Unresolved or nonlocal reference: " + componentPath);
        }
        VerifyDual(reopened, measured ? "DM_" : "DL_");
        ActivateConfiguration(reopened, DualLayout);
        View(reopened);
        Require(reopened.Save3((int)swSaveAsOptions_e.swSaveAsOptions_Silent, ref err, ref warn) && err == 0 && warn == 0,
            "Save verified assembly: " + err + "/" + warn);
        var spare = DualRim - (ShelfZ + ShelfT + BoardRest + BoardT + PlugSeat + Dupont);
        Near(spare, 4.8, "Nominal vertical wire bend allowance");
        Console.WriteLine("Nominal headroom above top connector: {0:F1} mm; actual bent wire not simulated.", spare);
        Console.WriteLine("Verified and open: " + path);
        return 0;
    }

    private static ModelDoc2 DualBase()
    {
        var doc = NewDoc(false);
        Add(doc, "Floor - case mounting and battery strap holes", 0, DualFloor, s =>
        {
            Outline(s, 45, 34, 5);
            Circles(s, DualCaseHoles, 2.1);
            Slot(s, 2.5, -30.25, 7, 1.8);
            Slot(s, 2.5, 6.2, 7, 2);
        });
        Add(doc, "2.4 mm walls", DualFloor, DualRim - DualFloor, s =>
        {
            Outline(s, 45, 34, 5);
            Outline(s, 42.6, 31.6, 3.6);
        });
        Posts(doc, "Lid posts - D2.5 pilots", DualLidPosts, 3.4, 1.25, DualFloor, DualRim, 8);
        Posts(doc, "Upper shelf posts - D2.5 pilots", ShelfPosts, 3.05, 1.25, DualFloor, ShelfZ, 6);
        Posts(doc, "IIM sensor - two D1.65 pilots on 19 mm centers", SensorPosts, 3, .825, DualFloor, 6.4, 3.6);
        Posts(doc, "LC76G provisional D1.65 pilots", GnssPosts, 2.65, .825, DualFloor, 6.4, 3.6);
        Add(doc, "GNSS un-drilled support", DualFloor, 3.6, s => Rect(s, -5.6, 23.4, -1.7, 28.2));
        Add(doc, "Battery locating end stops", DualFloor, 3, s =>
        {
            Rect(s, -37.4, -27.3, -35.7, -23.0);
            Rect(s, 33.7, -28, 36.2, -25.2);
        });
        Add(doc, "Power plate guide seats", 9.4, 2.4, s =>
        {
            Rect(s, -36.2, 26.8, -30.1, 32.1);
            Rect(s, 30.1, 26.8, 36.2, 32.1);
        });
        Add(doc, "Power plate rear guide channels", 11.8, 18.3, PowerGuideProfile);
        Add(doc, "Power plate guide ends - keeper washer pilots", 30.1, 4.5, s =>
        {
            PowerGuideProfile(s);
            Circles(s, new double[,] { { -34.1, 29.4 }, { 34.1, 29.4 } }, 1.05);
        });
        Add(doc, "Vertical cable ties - D2.5 guide eyes", 11, 2.6, s =>
        {
            Rect(s, -42.8, -8, -37.6, -2);
            Circles(s, new double[,] { { -40.05, -5 } }, 1.25);
        });
        Add(doc, "Upper cable tie eye", 22, 2.6, s =>
        {
            Rect(s, -42.8, -8, -37.6, -2);
            Circles(s, new double[,] { { -40.05, -5 } }, 1.25);
        });
        DualPort(doc, "USB access reserve 13 x 11", 42, 11, 13, s => Rect(s, 42, -17, 45.4, -4));
        DualPort(doc, "LoRa antenna reserve 16 x 12", 44, 12, 16, s => Rect(s, 16, -34.4, 32, -31));
        VerifyPart(doc, 90, 68, DualRim);
        return SavePart(doc, "DL_Base", new[] { .26, .32, .36 });
    }

    private static void PowerGuideProfile(SketchManager s)
    {
        Polygon(s, new double[,] { { 30.1, 26.8 }, { 36.2, 26.8 }, { 36.2, 32.1 },
            { 32.4, 32.1 }, { 32.4, 27.75 }, { 30.1, 27.75 } });
        Polygon(s, new double[,] { { -30.1, 26.8 }, { -36.2, 26.8 }, { -36.2, 32.1 },
            { -32.4, 32.1 }, { -32.4, 27.75 }, { -30.1, 27.75 } });
    }

    private static ModelDoc2 DualShelf()
    {
        var doc = NewDoc(false);
        Add(doc, "2.6 mm removable shelf - wiring notch and clamp holes", 0, ShelfT, s =>
        {
            Polygon(s, new double[,] { { -41.9, -24.5 }, { 41.9, -24.5 }, { 41.9, 10.6 },
                { 20, 10.6 }, { 20, 8.9 }, { -15, 8.9 }, { -15, 10.6 },
                { -41.9, 10.6 }, { -41.9, -1 }, { -34, -1 }, { -34, -13 }, { -41.9, -13 } });
            Circles(s, ShelfPosts, 1.65);
            Circles(s, ClipHoles, 1.15);
        });
        Add(doc, "Controller board edge rests", ShelfT, BoardRest, s =>
        {
            Rect(s, -30, -20.9, 30, -19.3);
            Rect(s, -30, 5.30, 30, 6.90);
        });
        VerifyPart(doc, 83.8, 35.1, ShelfT + BoardRest);
        return SavePart(doc, "DL_Upper_Shelf", new[] { .23, .51, .52 });
    }

    private static ModelDoc2 DualClip(string name = "DL_Edge_Clip")
    {
        var doc = NewDoc(false);
        Add(doc, "M2 screw clip body", 0, 5.8, s =>
        {
            Rect(s, -3.5, -1.7, 3.5, 1.8);
            Circles(s, new double[,] { { 0, 0 } }, 1.15);
        });
        Add(doc, "Board edge retaining toe", 3.9, 1.9, s => Rect(s, -3.5, 1.8, 3.5, 3.1));
        VerifyPart(doc, 7, 4.8, 5.8);
        return SavePart(doc, name, new[] { .63, .66, .66 });
    }

    private static ModelDoc2 DualPowerPlate(string name = "DL_Power_Plate")
    {
        var doc = NewDoc(false);
        Add(doc, "Four 3.4 mm power PCB slots - nominal pattern 50 x 17", 0, 2.2, s =>
        {
            Outline(s, 32, 11.4, .6);
            foreach (var x in new[] { -25d, 25d })
                foreach (var y in new[] { -8.5d, 8.5d }) Slot(s, x, y, 5.6, 3.4);
        });
        VerifyPart(doc, 64, 22.8, 2.2);
        return SavePart(doc, name, new[] { .26, .46, .56 });
    }

    private static ModelDoc2 DualLid()
    {
        var doc = NewDoc(false);
        Add(doc, "Flat cover with four D3.4 clearances", 0, DualLidT, s =>
        {
            Outline(s, 45, 34, 5);
            Circles(s, DualLidPosts, 1.7);
        });
        VerifyPart(doc, 90, 68, DualLidT);
        return SavePart(doc, "DL_Lid", new[] { .32, .39, .42 });
    }

    private static ModelDoc2 DualReferences()
    {
        var doc = NewDoc(false);
        ReferenceBox(doc, "AA holder 69 x 34 x 18", -35.5, -29, DualFloor, 33.5, 5, DualFloor + 18, .18, .2, .23);
        ReferenceBox(doc, "YBX board 24 x 24", 4.5, 6.5, 6.4, 28.5, 30.5, 8, .09, .47, .37);
        ReferenceBox(doc, "YBX jumper envelope", 9, 7.8, 8, 24, 10.8, 26, .19, .20, .24);
        ReferenceBox(doc, "GNSS board 30.5 x 25.5", -31.5, 5.8, 6.4, -1, 31.3, 8, .13, .39, .73);
        ReferenceBox(doc, "GNSS patch allowance only", -26.25, 11.8, 8, -6.25, 27.8, 12, .79, .67, .50);
        ReferenceBox(doc, "Power board 59.5 x 25.8", -29.75, 26.5, 10.3, 29.75, 28.1, 36.1, .24, .32, .44);
        ReferenceBox(doc, "Power horizontal connector envelope", -13, 9.7, 27, 18, 26.5, 32, .19, .21, .26);
        double board = ShelfZ + ShelfT + BoardRest;
        ReferenceBox(doc, "EoRa board 64 x 27", -32, -20.5, board, 32, 6.5, board + BoardT, .10, .54, .39);
        ReferenceBox(doc, "EoRa upper connector envelope A", -14, -20.2, board + BoardT,
            -4, -17.3, board + BoardT + PlugSeat + Dupont, .18, .2, .24);
        ReferenceBox(doc, "EoRa upper connector envelope B", 3, 3.1, board + BoardT,
            14, 6, board + BoardT + PlugSeat + Dupont, .18, .2, .24);
        Require(doc.ForceRebuild3(false), "Reference envelopes rebuild.");
        Require(((object[])((PartDoc)doc).GetBodies2((int)swBodyType_e.swSolidBody, false)).Length == 10,
            "Expected ten independent layout bodies.");
        View(doc);
        Save(doc, "DL_Layout_References.SLDPRT");
        return doc;
    }

    private static void ReferenceBox(ModelDoc2 doc, string name, double x, double y, double z, double x2, double y2,
        double z2, double r, double g, double b)
    {
        ReferenceShape(doc, name, z, z2, s => Rect(s, x, y, x2, y2), r, g, b);
    }

    private static void ReferenceShape(ModelDoc2 doc, string name, double z, double z2, Action<SketchManager> profile,
        double r, double g, double b)
    {
        ReferenceBody(doc, name, z, z2, profile, r, g, b);
    }

    private static Body2 ReferenceBody(ModelDoc2 doc, string name, double z, double z2, Action<SketchManager> profile,
        double r, double g, double b)
    {
        Sketch(doc, name, profile);
        var feat = doc.FeatureManager.FeatureExtrusion3(true, false, false, 0, 0, M(z2 - z), 0,
            false, false, false, false, 0, 0, false, false, false, false, false, false, true, 3, M(z), false);
        CheckFeature(feat, name);
        return PaintReference(feat, name, r, g, b);
    }

    private static Body2 PaintReference(Feature feat, string name, double r, double g, double b)
    {
        var faces = (object[])feat.GetFaces();
        var body = (Body2)((Face2)faces[0]).GetBody();
        body.Name = name;
        body.MaterialPropertyValues2 = new double[] { r, g, b, .75, .8, .25, .15, 0, 0 };
        return body;
    }

    private static void Posts(ModelDoc2 doc, string name, double[,] points, double r, double pilot,
        double low, double high, double pilotDepth)
    {
        if (high - low > pilotDepth + .0001)
            Add(doc, name + " core", low, high - low - pilotDepth, s => Circles(s, points, r));
        Add(doc, name, high - pilotDepth, pilotDepth, s =>
        {
            Circles(s, points, r);
            Circles(s, points, pilot);
        });
    }

    private static void DualPort(ModelDoc2 doc, string name, double top, double height, double width, Action<SketchManager> shape)
    {
        var before = VolumeMm3(doc);
        Sketch(doc, name, shape);
        var feature = doc.FeatureManager.FeatureCut4(true, false, false, 0, 0, M(height), 0,
            false, false, false, false, 0, 0, false, false, false, false,
            false, true, true, false, false, false, 3, M(top), false, false);
        CheckFeature(feature, name);
        Near(before - VolumeMm3(doc), 2.4 * width * height, name + " removal");
    }

    private static Component2 PlaceDual(ModelDoc2 doc, ModelDoc2 part, double[] rotation, double x, double y, double z)
    {
        var asm = (AssemblyDoc)doc;
        var item = asm.AddComponent5(part.GetPathName(), 0, "", false, "", 0, 0, 0);
        Require(item != null, "Insert " + part.GetTitle());
        var mu = (MathUtility)sw.GetMathUtility();
        double[] transform = rotation.Concat(new[] { M(x), M(y), M(z), 1d, 0d, 0d, 0d }).ToArray();
        if (item.IsFixed())
        {
            Require(item.Select4(false, null, false), "Select fixed component.");
            asm.UnfixComponent();
        }
        item.Transform2 = (MathTransform)mu.CreateTransform(transform);
        Require(item.Select4(false, null, false), "Select positioned component.");
        asm.FixComponent();
        doc.ClearSelection2(true);
        var current = (double[])item.Transform2.ArrayData;
        for (int i = 0; i <= 12; i++) Near(current[i] * 1000, transform[i] * 1000, part.GetTitle() + " transform " + i);
        return item;
    }

    private static Component2[] PartComponents(AssemblyDoc assembly, string prefix)
    {
        return ((object[])assembly.GetComponents(true)).Cast<Component2>().Where(c => c.Name2.StartsWith(prefix + "-", StringComparison.Ordinal)).ToArray();
    }

    private static void AddDualConfig(ModelDoc2 doc, string name, string description)
    {
        Require(doc.AddConfiguration3(name, description, "", 0) != null, "Add " + name);
        ActivateConfiguration(doc, name);
    }

    private static void SetDualSuppression(ModelDoc2 doc, Component2 item)
    {
        Require(item != null, "Missing component in " + doc.IGetActiveConfiguration().Name);
        string name = item.Name2;
        if (item.IsSuppressed()) return;
        Require(item.Select4(false, null, false), "Select " + name + " in " + doc.IGetActiveConfiguration().Name);
        Require(((AssemblyDoc)doc).SetComponentSuppression((int)swComponentSuppressionState_e.swComponentSuppressed), "Suppress " + name);
        doc.ClearSelection2(true);
    }

    private static void VerifyDual(ModelDoc2 doc, string prefix)
    {
        ActivateConfiguration(doc, DualClosed);
        CheckDualState(doc, 8, true);
        var asm = (AssemblyDoc)doc;
        var lid = PartComponents(asm, prefix + "Lid").Single();
        Near(1000 * ((double[])lid.Transform2.ArrayData)[11] + DualLidT, 60, "Closed height");
        ActivateConfiguration(doc, DualLayout);
        CheckDualState(doc, 8, true);
        ActivateConfiguration(doc, DualService);
        CheckDualState(doc, 2, true);
    }

    private static void CheckDualState(ModelDoc2 doc, int expected, bool checkInterference)
    {
        string config = doc.IGetActiveConfiguration().Name;
        Require(doc.ForceRebuild3(false), config + " rebuild.");
        var asm = (AssemblyDoc)doc;
        var components = ((object[])asm.GetComponents(true)).Cast<Component2>().Where(c => !c.IsSuppressed()).ToArray();
        Require(components.Length == expected, config + " part count: " + components.Length + " expected " + expected);
        Require(components.All(c => c.GetModelDoc2() != null && c.IsFixed()), config + " has unresolved or unfixed parts.");
        if (checkInterference)
        {
            var detection = asm.InterferenceDetectionManager;
            detection.TreatCoincidenceAsInterference = false;
            var collisions = detection.GetInterferences() as Array;
            if (collisions != null && collisions.Length > 0)
            {
                foreach (Interference hit in collisions)
                {
                    var touched = ((object[])hit.Components).Cast<Component2>().Select(c => c.Name2);
                    Console.Error.WriteLine(config + " collision: " + string.Join(" + ", touched) + " volume " + hit.Volume * 1e9 + " mm3");
                }
                throw new InvalidOperationException(config + " has " + collisions.Length + " solid interferences.");
            }
        }
        Console.WriteLine(config + ": " + expected + " components, 0 solid interferences.");
    }
}
