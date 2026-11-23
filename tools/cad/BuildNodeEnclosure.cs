using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using SolidWorks.Interop.sldworks;
using SolidWorks.Interop.swconst;

internal static partial class BuildNodeEnclosure
{
    private const double Rim = 51, Floor = 3.2, TrayZ = 8.2, TrayT = 3.2, LidLip = 4, LidT = 3.2;
    private const string Closed = "Assembled", Open = "Lid_Removed";
    private static readonly double[,] LidPosts = { { -76.5, -64.5 }, { -76.5, 64.5 }, { 76.5, -64.5 }, { 76.5, 64.5 } };
    private static readonly double[,] TrayPosts = { { -67, -55 }, { -67, 54 }, { 67, -55 }, { 67, 54 }, { 0, 8 }, { 70, 33 } };
    private static readonly double[,] CaseHoles = { { -55, -45 }, { -55, 45 }, { 55, -45 }, { 55, 45 } };
    private static ISldWorks sw;
    private static string output;

    [STAThread]
    private static int Main(string[] args)
    {
        CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;
        try
        {
            if (args.Length == 2 && args[1] == "--dual") return BuildDual(args[0]);
            if (args.Length == 2 && args[1] == "--dual-assemble") return BuildDual(args[0], true);
            if (args.Length == 2 && args[1] == "--dual-verify") return VerifySavedDual(args[0]);
            if (args.Length == 2 && args[1] == "--measured") return BuildMeasured(args[0]);
            if (args.Length == 2 && args[1] == "--measured-assemble") return BuildMeasured(args[0], true);
            if (args.Length == 2 && args[1] == "--measured-verify") return VerifySavedDual(args[0], true);
            if (args.Length == 2 && args[1] == "--printable") return BuildPrintable(args[0]);
            if (args.Length == 2 && args[1] == "--printable-assemble") return BuildPrintable(args[0], true);
            if (args.Length == 2 && args[1] == "--printable-verify") return VerifyPrintable(args[0]);
            bool assembleOnly = args.Length == 2 && args[1] == "--assemble";
            if (args.Length != 1 && !assembleOnly) throw new ArgumentException("Specify an empty output directory or an existing part directory followed by --assemble.");
            output = Path.GetFullPath(args[0]);
            Directory.CreateDirectory(output);
            Require(assembleOnly || !Directory.EnumerateFileSystemEntries(output).Any(), "Output directory is not empty.");
            Require(!File.Exists(Path.Combine(output, "Sensor_Node_Enclosure.SLDASM")), "Assembly already exists.");
            sw = (ISldWorks)Marshal.GetActiveObject("SldWorks.Application");
            Console.WriteLine("SolidWorks " + sw.RevisionNumber());
            var parts = assembleOnly ? new[] { LoadPart("Node_Base"), LoadPart("Node_Tray"), LoadPart("Node_Lid") }
                : new[] { MakeBase(), MakeTray(), MakeLid() };
            var assembly = MakeAssembly(parts);
            var path = assembly.GetPathName();
            sw.CloseDoc(assembly.GetTitle());
            foreach (var part in parts) sw.CloseDoc(part.GetTitle());
            int error = 0, warning = 0;
            var reopened = sw.OpenDoc6(path, (int)swDocumentTypes_e.swDocASSEMBLY,
                (int)swOpenDocOptions_e.swOpenDocOptions_Silent, Open, ref error, ref warning);
            Require(reopened != null && error == 0 && warning == 0, "Reopen: " + error + "/" + warning);
            CheckAssembly(reopened);
            ActivateConfiguration(reopened, Open);
            View(reopened);
            Console.WriteLine("Verified and open: " + path);
            return 0;
        }
        catch (Exception e)
        {
            Console.Error.WriteLine(e);
            return 1;
        }
    }

    private static ModelDoc2 MakeBase()
    {
        var doc = NewDoc(false);
        Add(doc, "Floor 3.2 - four case mounting holes D4.3", 0, Floor, s =>
        {
            Outline(s, 85, 73, 8);
            Circles(s, CaseHoles, 2.15);
        });
        Add(doc, "Perimeter wall 2.8", Floor, Rim - Floor, s =>
        {
            Outline(s, 85, 73, 8);
            Outline(s, 82.2, 70.2, 6.36);
        });
        Add(doc, "Lid post cores", Floor, 41 - Floor, s => Circles(s, LidPosts, 5.4));
        Add(doc, "Lid post tips - D2.6 pilot x 10", 41, 10, s =>
        {
            Circles(s, LidPosts, 5.4);
            Circles(s, LidPosts, 1.3);
        });
        Add(doc, "Tray supports - D2.6 pilot x 5", Floor, TrayZ - Floor, s =>
        {
            Circles(s, TrayPosts, 4.5);
            Circles(s, TrayPosts, 1.3);
        });
        double priorVolume = VolumeMm3(doc);
        CutNotches(doc, "Cable exits 18 x 10 and 12 x 10", s =>
        {
            Rect(s, -87, 20, -81, 38);
            Rect(s, 81, 23, 87, 35);
        });
        Near(priorVolume - VolumeMm3(doc), (18 + 12) * 2.8 * 10, "Cable notch volume");
        VerifyPart(doc, 170, 146, Rim);
        return SavePart(doc, "Node_Base", new[] { .15, .20, .22 });
    }

    private static ModelDoc2 MakeTray()
    {
        var doc = NewDoc(false);
        Add(doc, "3.2 mounting plate - screw and strap through slots", 0, TrayT, s =>
        {
            Outline(s, 76, 62, 5);
            Circles(s, TrayPosts, 1.7);
            // Main board and power module sit to the left; leave the center open for wiring.
            foreach (double y in new[] { 24d, 30d, 36d, 42d, 48d, -43d, -36d, -29d, -22d, -15d, -8d })
                foreach (double x in new[] { -62d, -44d, -26d, -11d })
                    Slot(s, x, y, 12, 3.3);
            foreach (double y in new[] { 22d, 28d, 34d, 40d, 46d, 52d })
                foreach (double x in new[] { 12d, 26d, 36d })
                    Slot(s, x, y, 9, 3.3);
            foreach (double y in new[] { -9d, -3d, 3d, 9d })
                foreach (double x in new[] { 29d, 43d, 57d })
                    Slot(s, x, y, 10, 3.3);
            foreach (double y in new[] { -57d, -15d })
                foreach (double x in new[] { 10d, 53d })
                    Slot(s, x, y, 16, 4);
            foreach (double y in new[] { -51d, -20d })
                foreach (double x in new[] { 13d, 34d, 54d })
                    Slot(s, x, y, 12, 3.3);
            // Leave X=41..68 / Y=18..52 solid for transfer drilling the sensor holes.
            foreach (double x in new[] { -54d, -24d, 20d, 47d })
                Slot(s, x, 16, 9, 2.8);
        });
        VerifyPart(doc, 152, 124, TrayT);
        return SavePart(doc, "Node_Tray", new[] { .19, .54, .48 });
    }

    private static ModelDoc2 MakeLid()
    {
        var doc = NewDoc(false);
        Add(doc, "Cover 3.2 - four screw clearances D3.4", LidLip, LidT, s =>
        {
            Outline(s, 85, 73, 8);
            Circles(s, LidPosts, 1.7);
        });
        Add(doc, "Locating lips - 0.45 wall clearance", 0, LidLip, s =>
        {
            Rect(s, -65, -69.75, 65, -68.15);
            Rect(s, -65, 68.15, 65, 69.75);
            Rect(s, -81.75, -51, -80.15, 19);
            Rect(s, -81.75, 39, -80.15, 51);
            Rect(s, 80.15, -51, 81.75, 22);
            Rect(s, 80.15, 36, 81.75, 51);
        });
        VerifyPart(doc, 170, 146, LidLip + LidT);
        return SavePart(doc, "Node_Lid", new[] { .38, .46, .49 });
    }

    private static ModelDoc2 MakeAssembly(ModelDoc2[] parts)
    {
        var doc = NewDoc(true);
        doc.IGetActiveConfiguration().Name = Closed;
        var asm = (AssemblyDoc)doc;
        Component2 lid = null;
        for (int i = 0; i < parts.Length; i++)
        {
            var comp = asm.AddComponent5(parts[i].GetPathName(), 0, "", false, "", 0, 0, 0);
            Require(comp != null, "Insert " + parts[i].GetPathName());
            double z = i == 1 ? TrayZ : i == 2 ? Rim - LidLip : 0;
            var math = (MathUtility)sw.GetMathUtility();
            var tr = (MathTransform)math.CreateTransform(new double[]
                { 1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, M(z), 1, 0, 0, 0 });
            if (comp.IsFixed() && i != 0)
            {
                Require(comp.Select4(false, null, false), "Select component to unfix.");
                asm.UnfixComponent();
            }
            comp.Transform2 = tr;
            if (!comp.IsFixed())
            {
                Require(comp.Select4(false, null, false), "Select component to fix.");
                asm.FixComponent();
            }
            if (i == 2) lid = comp;
            doc.ClearSelection2(true);
        }
        var config = (Configuration)doc.AddConfiguration3(Open, "Inspect tray and wiring space", "", 0);
        Require(config != null, "Add open configuration.");
        Console.WriteLine("Configurations: " + string.Join(", ", (string[])doc.GetConfigurationNames()));
        ActivateConfiguration(doc, Open);
        var lidName = lid.Name2;
        var openLid = asm.GetComponentByName(lidName);
        Require(openLid != null && openLid.Select4(false, null, false), "Select lid in open configuration.");
        Require(asm.SetComponentSuppression((int)swComponentSuppressionState_e.swComponentSuppressed), "Suppress lid in open configuration.");
        doc.ClearSelection2(true);
        CheckAssembly(doc);
        ActivateConfiguration(doc, Open);
        View(doc);
        Save(doc, "Sensor_Node_Enclosure.SLDASM");
        return doc;
    }

    private static void CheckAssembly(ModelDoc2 doc)
    {
        var asm = (AssemblyDoc)doc;
        ActivateConfiguration(doc, Closed);
        Require(doc.ForceRebuild3(false), "Assembly rebuild.");
        var comps = ((object[])asm.GetComponents(true)).Cast<Component2>().ToArray();
        Require(comps.Length == 3, "Expected three parts.");
        foreach (var part in comps)
        {
            string name = Path.GetFileNameWithoutExtension(part.GetPathName());
            Require(part.GetModelDoc2() != null && part.IsFixed(), "Missing or floating: " + name);
            var transform = (double[])part.Transform2.ArrayData;
            Near(transform[9] * 1000, 0, name + " X");
            Near(transform[10] * 1000, 0, name + " Y");
            Near(transform[11] * 1000, name == "Node_Lid" ? Rim - LidLip : name == "Node_Tray" ? TrayZ : 0, name + " Z");
        }
        var detector = asm.InterferenceDetectionManager;
        detector.TreatCoincidenceAsInterference = false;
        var overlaps = detector.GetInterferences() as Array;
        Require(overlaps == null || overlaps.Length == 0, "Solid part interference detected.");
        Console.WriteLine("Assembled: 3 fixed parts, 0 solid interferences.");
        ActivateConfiguration(doc, Open);
        var lid = ((object[])asm.GetComponents(true)).Cast<Component2>().Single(c => c.Name2.StartsWith("Node_Lid", StringComparison.Ordinal));
        Require(lid.GetSuppression2() ==
            (int)swComponentSuppressionState_e.swComponentSuppressed, "Lid should be removed in open configuration.");
    }

    private static ModelDoc2 LoadPart(string name)
    {
        var path = Path.Combine(output, name + ".SLDPRT");
        int error = 0, warning = 0;
        var part = (ModelDoc2)sw.GetOpenDocumentByName(path) ?? sw.OpenDoc6(path,
            (int)swDocumentTypes_e.swDocPART, (int)swOpenDocOptions_e.swOpenDocOptions_Silent,
            "", ref error, ref warning);
        Require(part != null && error == 0 && warning == 0, "Load " + path + ": " + error + "/" + warning);
        return part;
    }

    private static void ActivateConfiguration(ModelDoc2 doc, string name)
    {
        if (doc.IGetActiveConfiguration().Name != name) doc.ShowConfiguration2(name);
        Require(doc.IGetActiveConfiguration().Name == name, "Activate " + name + "; current=" + doc.IGetActiveConfiguration().Name);
    }

    private static ModelDoc2 NewDoc(bool assembly)
    {
        int pref = (int)(assembly ? swUserPreferenceStringValue_e.swDefaultTemplateAssembly : swUserPreferenceStringValue_e.swDefaultTemplatePart);
        var template = sw.GetUserPreferenceStringValue(pref);
        Require(File.Exists(template), "Missing SolidWorks template: " + template);
        var doc = (ModelDoc2)sw.NewDocument(template, 0, 0, 0);
        Require(doc != null, "Create document.");
        return doc;
    }

    private static void Sketch(ModelDoc2 doc, string name, Action<SketchManager> profile)
    {
        doc.ClearSelection2(true);
        var plane = (Feature)doc.FirstFeature();
        while (plane != null && plane.GetTypeName2() != "RefPlane") plane = (Feature)plane.GetNextFeature();
        Require(plane != null && plane.Select2(false, 0), "Select front plane.");
        var s = doc.SketchManager;
        s.InsertSketch(true);
        var sketch = s.ActiveSketch;
        Require(sketch != null, "Start sketch.");
        var feature = (Feature)sketch;
        s.AddToDB = true;
        try { profile(s); }
        finally { s.AddToDB = false; }
        s.InsertSketch(true);
        feature.Name = name + " profile";
        doc.ClearSelection2(true);
        Require(feature.Select2(false, 0), "Select finished sketch.");
    }

    private static void Add(ModelDoc2 doc, string name, double z, double depth, Action<SketchManager> profile)
    {
        Sketch(doc, name, profile);
        var feature = doc.FeatureManager.FeatureExtrusion3(true, false, false, 0, 0, M(depth), 0,
            false, false, false, false, 0, 0, false, false, false, false,
            true, true, true, z == 0 ? 0 : 3, M(z), false);
        CheckFeature(feature, name);
    }

    private static void CutNotches(ModelDoc2 doc, string name, Action<SketchManager> profile)
    {
        Sketch(doc, name, profile);
        var feature = doc.FeatureManager.FeatureCut4(true, false, false, 0, 0, M(10), 0,
            false, false, false, false, 0, 0, false, false, false, false,
            false, true, true, false, false, false, 3, M(Rim), false, false);
        CheckFeature(feature, name);
    }

    private static void CheckFeature(Feature feature, string name)
    {
        Require(feature != null, "Create " + name);
        bool warning;
        int code = feature.GetErrorCode2(out warning);
        Require(code == 0, name + ": " + code);
        feature.Name = name;
    }

    private static void Outline(SketchManager s, double a, double b, double c)
    {
        Polygon(s, new double[,] { { -a+c, -b }, { a-c, -b }, { a, -b+c }, { a, b-c },
            { a-c, b }, { -a+c, b }, { -a, b-c }, { -a, -b+c } });
    }

    private static void Rect(SketchManager s, double x1, double y1, double x2, double y2)
    {
        Polygon(s, new double[,] { { x1, y1 }, { x2, y1 }, { x2, y2 }, { x1, y2 } });
    }

    private static void Polygon(SketchManager s, double[,] p)
    {
        for (int i = 0; i < p.GetLength(0); i++)
        {
            int j = (i + 1) % p.GetLength(0);
            Line(s, p[i, 0], p[i, 1], p[j, 0], p[j, 1]);
        }
    }

    private static void Line(SketchManager s, double x1, double y1, double x2, double y2)
    {
        Require(s.CreateLine(M(x1), M(y1), 0, M(x2), M(y2), 0) != null, "Create line.");
    }

    private static void Circles(SketchManager s, double[,] points, double radius)
    {
        for (int i = 0; i < points.GetLength(0); i++)
            Require(s.CreateCircleByRadius(M(points[i, 0]), M(points[i, 1]), 0, M(radius)) != null, "Create circle.");
    }

    private static void Slot(SketchManager s, double x, double y, double length, double width)
    {
        double r = width / 2, left = x - length / 2 + r, right = x + length / 2 - r;
        Line(s, left, y - r, right, y - r);
        Require(s.Create3PointArc(M(right), M(y-r), 0, M(right), M(y+r), 0, M(right+r), M(y), 0) != null, "Right slot arc.");
        Line(s, right, y + r, left, y + r);
        Require(s.Create3PointArc(M(left), M(y+r), 0, M(left), M(y-r), 0, M(left-r), M(y), 0) != null, "Left slot arc.");
    }

    private static void VerifyPart(ModelDoc2 doc, double x, double y, double z)
    {
        Require(doc.ForceRebuild3(false), "Part rebuild.");
        var solid = ((object[])((PartDoc)doc).GetBodies2((int)swBodyType_e.swSolidBody, false)).Cast<Body2>().Single();
        var box = (double[])solid.GetBodyBox();
        Near(1000 * (box[3] - box[0]), x, "Body width");
        Near(1000 * (box[4] - box[1]), y, "Body depth");
        Near(1000 * (box[5] - box[2]), z, "Body height");
        Near(1000 * box[2], 0, "Body bottom");
        Console.WriteLine("One solid: {0} x {1} x {2} mm, {3:F0} mm3", x, y, z, VolumeMm3(doc));
    }

    private static double VolumeMm3(ModelDoc2 doc)
    {
        return doc.Extension.CreateMassProperty().Volume * 1e9;
    }

    private static ModelDoc2 SavePart(ModelDoc2 doc, string name, double[] rgb)
    {
        doc.MaterialPropertyValues = new double[] { rgb[0], rgb[1], rgb[2], .75, .8, .35, .2, 0, 0 };
        View(doc);
        Save(doc, name + ".SLDPRT");
        return doc;
    }

    private static void View(ModelDoc2 doc)
    {
        doc.ShowNamedView2("*Isometric", (int)swStandardViews_e.swIsometricView);
        doc.ViewZoomtofit2();
        doc.ClearSelection2(true);
    }

    private static void Save(ModelDoc2 doc, string name)
    {
        int error = 0, warning = 0;
        var path = Path.Combine(output, name);
        Require(doc.Extension.SaveAs3(path, 0, (int)swSaveAsOptions_e.swSaveAsOptions_Silent,
            null, null, ref error, ref warning) && error == 0 && warning == 0,
            "Save " + path + ": " + error + "/" + warning);
        Console.WriteLine("Saved " + path);
    }

    private static double M(double mm) { return mm / 1000; }
    private static void Near(double actual, double expected, string label)
    {
        Require(Math.Abs(actual - expected) < .03, label + ": " + actual + " != " + expected);
    }
    private static void Require(bool condition, string message)
    {
        if (!condition) throw new InvalidOperationException(message);
    }
}
