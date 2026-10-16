package generic

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Rekognition: nothing looks at the pixels. Every answer is made from the hash of the image, so
// the same image gives the same labels, face and text. Collections keep the faces indexed into them,
// and an image finds the faces that were indexed from the same image.

var labelNames = []string{"Person", "Clothing", "Outdoors", "Tree", "Plant", "Building", "Car", "Vehicle", "Dog", "Pet", "Animal", "Food", "Table",
	"Furniture", "Sky", "Water", "Text", "Computer", "Electronics", "Face", "Road", "Flower", "Cat", "Bicycle"}

var labelParents = map[string][]string{"Dog": {"Pet", "Animal"}, "Cat": {"Pet", "Animal"}, "Pet": {"Animal"}, "Car": {"Vehicle"}, "Bicycle": {"Vehicle"},
	"Tree": {"Plant"}, "Flower": {"Plant"}, "Table": {"Furniture"}, "Computer": {"Electronics"}}

var labelInstances = map[string]bool{"Person": true, "Car": true, "Dog": true, "Cat": true, "Bicycle": true}

// imageOf is the bytes of an Image: given inline, or read from S3 (a missing object is an error).
func imageOf(c *Call) ([]byte, error) { return imageMember(c, "Image") }

func imageMember(c *Call, member string) ([]byte, error) {
	img := aiAmap(c.In[member])
	if b, err := base64.StdEncoding.DecodeString(str(img["Bytes"])); err == nil && len(b) > 0 {
		return b, nil
	}
	if o := aiAmap(img["S3Object"]); o != nil {
		if b, ok := aiS3Get(c, str(o["Bucket"]), str(o["Name"])); ok {
			return b, nil
		}
		return nil, core.Errorf(400, "InvalidS3ObjectException", "Unable to get object metadata from S3. Check object key, region and/or access permissions.")
	}
	return nil, c.Invalid("Image must have Bytes or S3Object")
}

func aiUuidOf(parts ...any) string {
	sum := sha256.Sum256([]byte(fmt.Sprint(parts...)))
	return fmt.Sprintf("%x-%x-%x-%x-%x", sum[:4], sum[4:6], sum[6:8], sum[8:10], sum[10:16])
}

func faceBox() map[string]any {
	return map[string]any{"Width": 0.3, "Height": 0.4, "Left": 0.35, "Top": 0.2}
}

// faceDetail is the one face every image has.
func faceDetail(img []byte) map[string]any {
	sum := sha256.Sum256(img)
	gender := []string{"Male", "Female"}[sum[0]%2]
	return map[string]any{"BoundingBox": faceBox(), "Confidence": 99.9, "AgeRange": map[string]any{"Low": 25, "High": 35},
		"Smile": map[string]any{"Value": sum[1]%2 == 0, "Confidence": 95.0}, "Gender": map[string]any{"Value": gender, "Confidence": 97.0},
		"Emotions": []any{map[string]any{"Type": "CALM", "Confidence": 90.0}, map[string]any{"Type": "HAPPY", "Confidence": 8.0}},
		"Pose":     map[string]any{"Roll": 0.5, "Yaw": -2.0, "Pitch": 3.0}, "Quality": map[string]any{"Brightness": 80.0, "Sharpness": 90.0}}
}

func collectionFaces(r *Resource) []any { return asList(r.Data["Faces"]) }

func init() {
	const svc = "rekognition"

	Override(svc, "DetectLabels", func(c *Call) (map[string]any, error) {
		img, err := imageOf(c)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(img)
		limit := 4
		if n := int(aiNum(c.In["MaxLabels"])); n > 0 {
			limit = min(n, len(labelNames))
		}
		min_ := aiNum(c.In["MinConfidence"])
		var labels []any
		seen := map[string]bool{}
		for i := 0; len(labels) < limit && i < len(sum); i++ {
			name := labelNames[int(sum[i])%len(labelNames)]
			conf := 99.0 - float64(len(labels))*3.5
			if seen[name] || conf < min_ {
				continue
			}
			seen[name] = true
			l := map[string]any{"Name": name, "Confidence": conf, "Instances": []any{}, "Parents": []any{}, "Aliases": []any{}, "Categories": []any{}}
			for _, p := range labelParents[name] {
				l["Parents"] = append(l["Parents"].([]any), map[string]any{"Name": p})
			}
			if labelInstances[name] {
				l["Instances"] = []any{map[string]any{"BoundingBox": map[string]any{"Width": 0.4, "Height": 0.6, "Left": 0.3, "Top": 0.2}, "Confidence": conf}}
			}
			labels = append(labels, l)
		}
		return map[string]any{"Labels": labels, "LabelModelVersion": "3.0"}, nil
	})

	Override(svc, "DetectFaces", func(c *Call) (map[string]any, error) {
		img, err := imageOf(c)
		if err != nil {
			return nil, err
		}
		return map[string]any{"FaceDetails": []any{faceDetail(img)}}, nil
	})

	// Text: the lines of a text image, else one canned line.
	Override(svc, "DetectText", func(c *Call) (map[string]any, error) {
		img, err := imageOf(c)
		if err != nil {
			return nil, err
		}
		var lines []string
		if aiIsText(img) {
			for _, l := range strings.Split(string(img), "\n") {
				if l = strings.TrimSpace(l); l != "" {
					lines = append(lines, l)
				}
			}
		}
		if len(lines) == 0 {
			sum := sha256.Sum256(img)
			lines = []string{fmt.Sprintf("POGI CANNED %X", sum[:4])}
		}
		var out []any
		id := 0
		for i, l := range lines {
			lineID := id
			top := min(0.05+float64(i)*0.05, 0.9)
			out = append(out, map[string]any{"DetectedText": l, "Type": "LINE", "Id": lineID, "Confidence": 99.2, "Geometry": aiGeometry(top, 0.05, 0.9, 0.04)})
			id++
			for j, w := range strings.Fields(l) {
				out = append(out, map[string]any{"DetectedText": w, "Type": "WORD", "Id": id, "ParentId": lineID, "Confidence": 99.2, "Geometry": aiGeometry(top, 0.05+float64(j)*0.08, 0.07, 0.04)})
				id++
			}
		}
		return map[string]any{"TextDetections": out, "TextModelVersion": "3.0"}, nil
	})

	Override(svc, "DetectModerationLabels", func(c *Call) (map[string]any, error) {
		if _, err := imageOf(c); err != nil {
			return nil, err
		}
		return map[string]any{"ModerationLabels": []any{}, "ModerationModelVersion": "7.0"}, nil
	})
	Override(svc, "RecognizeCelebrities", func(c *Call) (map[string]any, error) {
		if _, err := imageOf(c); err != nil {
			return nil, err
		}
		return map[string]any{"CelebrityFaces": []any{}, "UnrecognizedFaces": []any{map[string]any{"BoundingBox": faceBox(), "Confidence": 99.9}}}, nil
	})

	// The same image is the same face; any other pair is not a match.
	Override(svc, "CompareFaces", func(c *Call) (map[string]any, error) {
		src, err := imageMember(c, "SourceImage")
		if err != nil {
			return nil, err
		}
		dst, err := imageMember(c, "TargetImage")
		if err != nil {
			return nil, err
		}
		face := map[string]any{"BoundingBox": faceBox(), "Confidence": 99.9}
		out := map[string]any{"SourceImageFace": face, "FaceMatches": []any{}, "UnmatchedFaces": []any{}}
		if string(src) == string(dst) {
			out["FaceMatches"] = []any{map[string]any{"Similarity": 99.9, "Face": face}}
		} else {
			out["UnmatchedFaces"] = []any{face}
		}
		return out, nil
	})

	// Collections: the engine keeps them; they also keep their faces.
	aiSeed(svc, "CreateCollection", func(c *Call, r *Resource) {
		r.Data["StatusCode"], r.Data["FaceModelVersion"], r.Data["FaceCount"], r.Data["UserCount"], r.Data["CollectionArn"] = 200.0, "7.0", 0.0, 0.0, r.ARN
	})
	Override(svc, "DeleteCollection", func(c *Call) (map[string]any, error) {
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		return map[string]any{"StatusCode": 200.0}, nil
	})
	collection := func(c *Call) (*Resource, error) {
		r := c.Get("Collection", str(c.In["CollectionId"]))
		if r == nil {
			return nil, c.NotFound("The collection id: %s does not exist", str(c.In["CollectionId"]))
		}
		return r, nil
	}
	Override(svc, "IndexFaces", func(c *Call) (map[string]any, error) {
		r, err := collection(c)
		if err != nil {
			return nil, err
		}
		img, err := imageOf(c)
		if err != nil {
			return nil, err
		}
		imageID := aiUuidOf("image", string(img))
		face := map[string]any{"FaceId": aiUuidOf("face", string(img), len(collectionFaces(r))), "ImageId": imageID, "BoundingBox": faceBox(), "Confidence": 99.9}
		if e := str(c.In["ExternalImageId"]); e != "" {
			face["ExternalImageId"] = e
		}
		r.Data["Faces"] = append(collectionFaces(r), face)
		r.Data["FaceCount"] = float64(len(collectionFaces(r)))
		c.Put(r)
		return map[string]any{"FaceRecords": []any{map[string]any{"Face": face, "FaceDetail": faceDetail(img)}}, "FaceModelVersion": "7.0", "UnindexedFaces": []any{}}, nil
	})
	Override(svc, "ListFaces", func(c *Call) (map[string]any, error) {
		r, err := collection(c)
		if err != nil {
			return nil, err
		}
		return map[string]any{"Faces": collectionFaces(r), "FaceModelVersion": "7.0"}, nil
	})
	Override(svc, "DeleteFaces", func(c *Call) (map[string]any, error) {
		r, err := collection(c)
		if err != nil {
			return nil, err
		}
		drop := aiStrList(c.In["FaceIds"])
		var keep, gone []any
		for _, f := range collectionFaces(r) {
			if slices.Contains(drop, str(aiAmap(f)["FaceId"])) {
				gone = append(gone, str(aiAmap(f)["FaceId"]))
			} else {
				keep = append(keep, f)
			}
		}
		r.Data["Faces"], r.Data["FaceCount"] = keep, float64(len(keep))
		c.Put(r)
		return map[string]any{"DeletedFaces": gone}, nil
	})
	matches := func(r *Resource, imageID, except string) []any {
		var out []any
		for _, f := range collectionFaces(r) {
			if fm := aiAmap(f); str(fm["ImageId"]) == imageID && str(fm["FaceId"]) != except {
				out = append(out, map[string]any{"Similarity": 99.9, "Face": fm})
			}
		}
		return out
	}
	Override(svc, "SearchFacesByImage", func(c *Call) (map[string]any, error) {
		r, err := collection(c)
		if err != nil {
			return nil, err
		}
		img, err := imageOf(c)
		if err != nil {
			return nil, err
		}
		return map[string]any{"FaceMatches": matches(r, aiUuidOf("image", string(img)), ""), "FaceModelVersion": "7.0", "SearchedFaceBoundingBox": faceBox(), "SearchedFaceConfidence": 99.9}, nil
	})
	Override(svc, "SearchFaces", func(c *Call) (map[string]any, error) {
		r, err := collection(c)
		if err != nil {
			return nil, err
		}
		id := str(c.In["FaceId"])
		for _, f := range collectionFaces(r) {
			if fm := aiAmap(f); str(fm["FaceId"]) == id {
				return map[string]any{"SearchedFaceId": id, "FaceMatches": matches(r, str(fm["ImageId"]), id), "FaceModelVersion": "7.0"}, nil
			}
		}
		return nil, c.NotFound("face %s does not exist", id)
	})
}
