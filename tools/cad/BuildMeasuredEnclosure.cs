using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using SolidWorks.Interop.sldworks;
using SolidWorks.Interop.swconst;

internal static partial class BuildNodeEnclosure
{
    private const string MeasuredFile = "Dual_Layer_Measured_84x66.SLDASM";
    private const double MeasHalfX = 42, MeasHalfY = 33, MeasWall = 2.4;
    private const double BatteryRest = 1.2;
    private const double GnssX = -31.3, GnssY = 4.7, GnssW = 30.5, GnssD = 25.5, GnssZ = 6.4, GnssH = 11.5;
    private const double PowerY = 29.3, PowerZ = 32;
    private const double BoardBottomX = -32, BoardEdgeY = -20, BoardWidth = 27;
    private const double BoardTopZ = ShelfZ + ShelfT + BoardRest + BoardT;
    private const double AntennaX = BoardBottomX + 18, AntennaSeatH = 10.6;
    private const double AntennaZ = BoardTopZ + AntennaSeatH / 2;
    private const double AntennaDiameter = 6.5, PortDiameter = 7;
    private static readonly double[,] MeasLid = { { -37.5, -26.5 }, { 37.5, -26.5 }, { -37.5, 26.5 }, { 37.5, 26.5 } };
    private static readonly double[,] MeasShelfPosts = { { -36.8, -17.6 }, { 36.8, -17.6 }, { -36.8, 2.2 }, { 36.8, 2.2 } };
    private static readonly double[,] MeasClip = { { -23, -22.2 }, { 24, -22.2 }, { -23, 9.2 }, { 24, 9.2 } };
    private static readonly double[,] MeasGnss = { { GnssX + 2.25, 7 }, { GnssX + 2.25, 27.8 } };
    private static readonly double[,] MeasSensor = { { 7, 27 }, { 26, 27 } };
    private static readonly double[,] MeasMount = { { -37.2, -9 }, { 37.2, -9 }, { -37.2, 17 }, { 37.2, 17 } };

    private static int BuildMeasured(string directory, bool assembleOnly = false)
    {
        output = Path.GetFullPath(directory);
        Directory.CreateDirectory(output);
        Require(assembleOnly || !Directory.EnumerateFileSystemEntries(output).Any(), "Output directory is not empty.");
        Require(!File.Exists(Path.Combine(output, MeasuredFile)), "Measured assembly already exists.");
        sw = (ISldWorks)Marshal.GetActiveObject("SldWorks.Application");
        Console.WriteLine("SolidWorks " + sw.RevisionNumber());
        var parts = assembleOnly ? new[] { LoadPart("DM_Base"), LoadPart("DM_Upper_Shelf"), LoadPart("DM_Edge_Clip"),
            LoadPart("DM_Power_Plate"), LoadPart("DM_Lid"), LoadPart("DM_Layout_References") }
            : new[] { MeasuredBase(), MeasuredShelf(), DualClip("DM_Edge_Clip"), DualPowerPlate("DM_Power_Plate"),
                MeasuredLid(), MeasuredReferences() };
        return AssembleDual(parts, MeasClip, PowerY, PowerZ, true);
    }

    private static ModelDoc2 MeasuredBase()
    {
        var doc = NewDoc(false);
        Add(doc, "84 x 66 floor with case screw and battery strap holes", 0, DualFloor, s =>
        {
            Outline(s, MeasHalfX, MeasHalfY, 4.5);
            Circles(s, MeasMount, 2.1);
            Slot(s, 2.5, -29.85, 7, 1.4);
            Slot(s, 2.5, 5.8, 7, 1.8);
        });
        Add(doc, "2.4 mm perimeter wall", DualFloor, DualRim - DualFloor, s =>
        {
            Outline(s, MeasHalfX, MeasHalfY, 4.5);
            Outline(s, MeasHalfX - MeasWall, MeasHalfY - MeasWall, 3.1);
        });
        Posts(doc, "Lid D2.2 pilots", MeasLid, 2.6, 1.1, DualFloor, DualRim, 8);
        Add(doc, "Wall-backed shelf support ribs", DualFloor, ShelfZ - DualFloor - 6, s => MeasuredShelfPads(s, false));
        Add(doc, "Wall-backed shelf support D1.65 pilots", ShelfZ - 6, 6, s => MeasuredShelfPads(s, true));
        Posts(doc, "Sensor D1.65 pilots - 19 mm centers", MeasSensor, 2.8, .825, DualFloor, 6.4, 3.6);
        Posts(doc, "GNSS M2 D1.65 pilots - centers provisional", MeasGnss, 2.65, .825, DualFloor, GnssZ, 3.6);
        Add(doc, "GNSS plain support", DualFloor, GnssZ - DualFloor, s => Rect(s, -5.6, 22.3, -1.7, 27.0));
        Add(doc, "Battery end stops", DualFloor, 3, s =>
        {
            Rect(s, -36.6, -26.9, -34.85, -23.2);
            Rect(s, 34.85, -27.1, 36.6, -23.2);
        });
        Add(doc, "Battery supports with strap space below", DualFloor, BatteryRest, s =>
        {
            Rect(s, -27, -27.5, -22, 2);
            Rect(s, 22, -27.5, 27, 2);
        });
        Add(doc, "Power plate raised guide seats", PowerZ - 13.8, 2.4, s =>
        {
            Rect(s, -36.2, 25.8, -30.1, 31.1);
            Rect(s, 30.1, 25.8, 36.2, 31.1);
        });
        Add(doc, "Power plate raised guide channels", PowerZ - 11.4, 18.3, MeasuredPowerGuide);
        Add(doc, "Power keeper washer D2.1 pilots", PowerZ + 6.9, 4.5, s =>
        {
            MeasuredPowerGuide(s);
            Circles(s, new double[,] { { -34.1, 28.4 }, { 34.1, 28.4 } }, 1.05);
        });
        foreach (double z in new[] { 11d, 22d })
            Add(doc, "Wall wiring tie eye at " + z, z, 2.6, s =>
            {
                Rect(s, -39.8, -9.6, -35.1, -3.8);
                Circles(s, new double[,] { { -37.4, -6.7 } }, 1.15);
            });
        DualPort(doc, "USB reserve 13 x 11 at lower end of PCB", 42, 11, 13, s => Rect(s, -42.4, -13, -39, 0));
        AddMeasuredRoundPort(doc);
        VerifyPart(doc, 84, 66, DualRim);
        return SavePart(doc, "DM_Base", new[] { .26, .32, .36 });
    }

    private static void MeasuredShelfPads(SketchManager s, bool pilots)
    {
        foreach (double y in new[] { -17.6, 2.2 })
        {
            Rect(s, 34.85, y - 1.95, 40.0, y + 1.95);
            Rect(s, -40.0, y - 1.95, -34.85, y + 1.95);
        }
        if (pilots) Circles(s, MeasShelfPosts, .825);
    }

    private static void MeasuredPowerGuide(SketchManager s)
    {
        foreach (double sign in new[] { -1d, 1d })
            Polygon(s, new double[,] { { sign * 30.1, 25.8 }, { sign * 36.2, 25.8 }, { sign * 36.2, 31.1 },
                { sign * 32.4, 31.1 }, { sign * 32.4, 26.75 }, { sign * 30.1, 26.75 } });
    }

    private static ModelDoc2 MeasuredShelf()
    {
        var doc = NewDoc(false);
        Add(doc, "Trimmed upper shelf - cable notch and antenna relief", 0, ShelfT, s =>
        {
            Polygon(s, new double[,] { { -38.8, -21.5 }, { -34.7, -24.0 }, { -18, -24.0 },
                { -18, -19.6 }, { -10, -19.6 }, { -10, -24.0 }, { 34.7, -24.0 }, { 38.8, -21.5 },
                { 38.8, 11.2 }, { 18.5, 11.2 }, { 18.5, 8.6 }, { -17.5, 8.6 }, { -17.5, 11.2 },
                { -38.8, 11.2 }, { -38.8, -.2 }, { -33.7, -.2 }, { -33.7, -13.1 }, { -38.8, -13.1 } });
            Circles(s, MeasShelfPosts, 1.15);
            Circles(s, MeasClip, 1.15);
        });
        Add(doc, "PCB edge rests with antenna gap", ShelfT, BoardRest, s =>
        {
            Rect(s, -30, -20.4, -18.3, -18.8);
            Rect(s, -9.7, -20.4, 30, -18.8);
            Rect(s, -30, 5.8, 30, 7.4);
        });
        VerifyPart(doc, 77.6, 35.2, ShelfT + BoardRest);
        return SavePart(doc, "DM_Upper_Shelf", new[] { .23, .51, .52 });
    }

    private static ModelDoc2 MeasuredLid()
    {
        var doc = NewDoc(false);
        Add(doc, "84 x 66 cover - four D2.8 holes", 0, DualLidT, s =>
        {
            Outline(s, MeasHalfX, MeasHalfY, 4.5);
            Circles(s, MeasLid, 1.4);
        });
        VerifyPart(doc, 84, 66, DualLidT);
        return SavePart(doc, "DM_Lid", new[] { .32, .39, .42 });
    }

    private static ModelDoc2 MeasuredReferences()
    {
        var doc = NewDoc(false);
        ReferenceBox(doc, "Battery holder 69 x 34 x 18", -34.5, -29.75, DualFloor + BatteryRest,
            34.5, 4.25, DualFloor + BatteryRest + 18, .18, .2, .23);
        ReferenceBox(doc, "YBX board 24 x 24", 4.5, 5.5, 6.4, 28.5, 29.5, 8, .09, .47, .37);
        ReferenceBox(doc, "YBX jumper allowance", 9, 7, 8, 24, 10, 26, .19, .20, .24);
        ReferenceShape(doc, "GNSS 11.5 mm high - 3 mm projection - D2 mount holes", GnssZ, GnssZ + GnssH, s =>
        {
            double right = GnssX + GnssW;
            Polygon(s, new double[,] { { GnssX, GnssY }, { right, GnssY }, { right, 13 }, { right + 3, 13 },
                { right + 3, 20 }, { right, 20 }, { right, GnssY + GnssD }, { GnssX, GnssY + GnssD } });
            Circles(s, MeasGnss, 1);
        }, .13, .39, .73);
        ReferenceBox(doc, "Power board 59.5 x 25.8", -29.75, PowerY - 3.8, PowerZ - 12.9,
            29.75, PowerY - 2.2, PowerZ + 12.9, .24, .32, .44);
        ReferenceBox(doc, "Power horizontal connector allowance", -13, 9.5, 30, 18, PowerY - 3.8, 35, .19, .21, .26);
        ReferenceShape(doc, "EoRa 64 x 27 - top projection 3 x 5.5", BoardTopZ - BoardT, BoardTopZ, s =>
        {
            double top = BoardBottomX + 64, yMid = BoardEdgeY + BoardWidth / 2;
            Polygon(s, new double[,] { { BoardBottomX, BoardEdgeY }, { top, BoardEdgeY }, { top, yMid - 2.75 },
                { top + 3, yMid - 2.75 }, { top + 3, yMid + 2.75 }, { top, yMid + 2.75 },
                { top, BoardEdgeY + BoardWidth }, { BoardBottomX, BoardEdgeY + BoardWidth } });
        }, .10, .54, .39);
        var seat = ReferenceBody(doc, "EoRa antenna housing 6 x 9 x 10.6", BoardTopZ, BoardTopZ + AntennaSeatH,
            s => Rect(s, AntennaX - 3, BoardEdgeY, AntennaX + 3, BoardEdgeY + 9), .61, .48, .24);
        const string gaugeName = "D6.5 antenna passage gauge - length not a part dimension";
        FaceCircle(doc, NegativeYFace(seat, BoardEdgeY), gaugeName, AntennaX, BoardEdgeY, AntennaZ, AntennaDiameter / 2);
        var gauge = doc.FeatureManager.FeatureExtrusion3(true, false, false, 0, 0, M(MeasHalfY + BoardEdgeY), 0,
            false, false, false, false, 0, 0, false, false, false, false, false, false, true, 0, 0, false);
        CheckFeature(gauge, gaugeName);
        var gaugeBody = PaintReference(gauge, gaugeName, .65, .49, .17);
        var box = (double[])gaugeBody.GetBodyBox();
        Near(box[1] * 1000, -MeasHalfY, "Gauge outer end");
        Near(box[4] * 1000, BoardEdgeY, "Gauge inner end");
        Near((box[3] - box[0]) * 1000, AntennaDiameter, "Gauge diameter");
        ReferenceBox(doc, "EoRa jumper allowance A", -9.6, -19.7, BoardTopZ, -2, -17, BoardTopZ + PlugSeat + Dupont, .18, .2, .24);
        ReferenceBox(doc, "EoRa jumper allowance B", 3, 3.5, BoardTopZ, 14, 6.4, BoardTopZ + PlugSeat + Dupont, .18, .2, .24);
        Require(doc.ForceRebuild3(false), "Measured reference rebuild.");
        Require(Solids(doc).Length == 11, "Expected eleven measured arrangement bodies.");
        View(doc);
        Save(doc, "DM_Layout_References.SLDPRT");
        return doc;
    }

    private static void AddMeasuredRoundPort(ModelDoc2 doc)
    {
        const string name = "D7 round port for D6.5 antenna - 18 from PCB bottom";
        double start = VolumeMm3(doc);
        FaceCircle(doc, NegativeYFace(Solids(doc).Single(), -MeasHalfY), name, AntennaX, -MeasHalfY, AntennaZ, PortDiameter / 2);
        var cut = doc.FeatureManager.FeatureCut4(true, false, false, 0, 0, M(MeasWall + .1), 0,
            false, false, false, false, 0, 0, false, false, false, false,
            false, true, true, false, false, false, 0, 0, false, false);
        CheckFeature(cut, name);
        Near(start - VolumeMm3(doc), Math.PI * PortDiameter * PortDiameter * MeasWall / 4, "Round port material removed");
    }

    private static Body2[] Solids(ModelDoc2 doc)
    {
        return ((object[])((PartDoc)doc).GetBodies2((int)swBodyType_e.swSolidBody, false)).Cast<Body2>().ToArray();
    }

    private static Face2 NegativeYFace(Body2 body, double y)
    {
        return ((object[])body.GetFaces()).Cast<Face2>().Single(f =>
        {
            var n = (double[])f.Normal;
            var bounds = (double[])f.GetBox();
            return n != null && Math.Abs(n[1] + 1) < 1e-7 && Math.Abs(bounds[1] * 1000 - y) < .001;
        });
    }

    private static void FaceCircle(ModelDoc2 doc, Face2 face, string name, double x, double y, double z, double radius)
    {
        doc.ClearSelection2(true);
        Require(((Entity)face).Select4(false, null), "Select face for " + name);
        var manager = doc.SketchManager;
        manager.InsertSketch(true);
        var sketch = manager.ActiveSketch;
        Require(sketch != null, "Start face sketch for " + name);
        var mu = (MathUtility)sw.GetMathUtility();
        var point = (MathPoint)mu.CreatePoint(new[] { M(x), M(y), M(z) });
        var local = (double[])point.IMultiplyTransform(sketch.ModelToSketchTransform).ArrayData;
        Near(1000 * local[2], 0, "Circle center in sketch plane");
        manager.AddToDB = true;
        try { Require(manager.CreateCircleByRadius(local[0], local[1], 0, M(radius)) != null, "Face circle for " + name); }
        finally { manager.AddToDB = false; }
        manager.InsertSketch(true);
        var feature = (Feature)sketch;
        feature.Name = name + " profile";
        doc.ClearSelection2(true);
        Require(feature.Select2(false, 0), "Select circular profile for " + name);
    }
}
